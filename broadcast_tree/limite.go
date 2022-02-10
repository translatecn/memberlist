package broadcast_tree

import (
	"github.com/google/btree"
	"github.com/hashicorp/memberlist/pkg"
	"math"
	"sync"
)

// -------------------------------------------- OK -----------------------------------------------

// TransmitLimitedQueue
// 用于对消息进行队列，以便(通过gossip)向集群广播，但限制每条消息的传输数量。它还对传输计数较低的消息(因此是较新的消息)进行优先级排序。

type TransmitLimitedQueue struct {
	// NumNodes 返回集群中的节点数。这用于确定根据此日志计算的重传计数。
	NumNodes func() int
	// RetransmitMult 用于确定重传的最大次数的 重传系数。
	RetransmitMult int
	mu             sync.Mutex
	tq             *btree.BTree                 // stores *limitedBroadcast as btree.Item
	tm             map[string]*limitedBroadcast // 节点 --> 广播消息
	idGen          int64
}

// 添加消息，同时使存在的消息Finished
func (q *TransmitLimitedQueue) queueBroadcast(b Broadcast, initialTransmits int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.lazyInit()
	// myself
	switch b.(type) {
	case *MemberlistBroadcast:
	default:
	}

	if q.idGen == math.MaxInt64 {
		//确保idGen不会超过MaxInt64,重置
		q.idGen = 1
	} else {
		q.idGen++
	}
	id := q.idGen

	lb := &limitedBroadcast{
		transmits: initialTransmits, // 传输次数
		msgLen:    int64(len(b.Message())),
		id:        id,
		B:         b,
	}
	unique := false
	if nb, ok := b.(NamedBroadcast); ok {
		var _ NamedBroadcast = &MemberlistBroadcast{}
		lb.name = nb.Name()
	} else if _, ok := b.(UniqueBroadcast); ok {
		// UniqueBroadcast 没有具体实现
		unique = true
	}

	// 检查这个消息是否使另一个消息无效。
	if lb.name != "" {
		// 判断
		if old, ok := q.tm[lb.name]; ok {
			old.B.Finished()
			q.DeleteItem(old)
		}
	} else if !unique {
		// lb.name == "" && unique == false
		// 消息没有命名、且不是UniqueBroadcast的实现
		// 有重复消息 干掉它们,返回结果
		var remove []*limitedBroadcast
		q.tq.Ascend(func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)
			switch cur.B.(type) {
			case NamedBroadcast:
				// noop
			case UniqueBroadcast:
				// noop
			default:
				if b.Invalidates(cur.B) {
					cur.B.Finished()
					remove = append(remove, cur)
				}
			}
			return true
		})
		for _, cur := range remove {
			q.DeleteItem(cur)
		}
	}

	//入队
	q.addItem(lb)
}

// Prune 将保留maxRetain的最新信息，其余的将被丢弃。这可以用来防止无限制的队列广播大小
func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.tq.Len() > maxRetain {
		item := q.tq.Max()
		if item == nil {
			break
		}
		cur := item.(*limitedBroadcast)
		cur.B.Finished()
		q.DeleteItem(cur)
	}
}

// lenLocked 返回BTree的长度
func (q *TransmitLimitedQueue) lenLocked() int {
	if q.tq == nil {
		return 0
	}
	return q.tq.Len()
}

// Reset  清除所有消息，计数器清零，仅用于测试,清空 BTree 以及 map[string]*limitedBroadcast
func (q *TransmitLimitedQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.walkReadOnlyLocked(false, func(cur *limitedBroadcast) bool {
		cur.B.Finished()
		return true
	})

	q.tq = nil
	q.tm = nil
	q.idGen = 0
}

// NumQueued 返回入队的消息
func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lenLocked()
}

// lazyInit 初始化内部数据结构
func (q *TransmitLimitedQueue) lazyInit() {
	if q.tq == nil {
		q.tq = btree.New(32)
	}
	if q.tm == nil {
		q.tm = make(map[string]*limitedBroadcast)
	}
}

// QueueBroadcast 广播消息入队,重试次数为0
func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.queueBroadcast(b, 0) // 初始重传次数为0
}

// OrderedView  用于测试；如果reverse=false，则按顺序发送
func (q *TransmitLimitedQueue) OrderedView(reverse bool) []*limitedBroadcast {
	// 程序里都是true,即升序
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.lenLocked())
	q.walkReadOnlyLocked(reverse, func(cur *limitedBroadcast) bool {
		out = append(out, cur)
		return true // 如果返回FALSE将不会继续遍历
	})

	return out
}

// walkReadOnlyLocked  如果你试图在遍历过程中改变该BTree，该方法就会panic。
// 底层的btree也不应该在遍历过程中被改变。
// 调用时持锁
func (q *TransmitLimitedQueue) walkReadOnlyLocked(reverse bool, f func(*limitedBroadcast) bool) {
	// 默认按照升序遍历BTree
	if q.lenLocked() == 0 {
		return
	}

	iter := func(item btree.Item) bool {
		cur := item.(*limitedBroadcast)
		prevTransmits := cur.transmits // 重试次数
		prevMsgLen := cur.msgLen       // 消息长度
		prevID := cur.id               // 消息ID

		keepGoing := f(cur)
		if prevTransmits != cur.transmits || prevMsgLen != cur.msgLen || prevID != cur.id {
			// 这里应该是走不到
			panic("遍历时发生了修改")
		}
		return keepGoing
	}

	if reverse {
		q.tq.Descend(iter) // end with transmit 0
	} else {
		q.tq.Ascend(iter) // start with transmit 0
	}
}

// GetBroadcasts 是用来获取一些广播的，最多不超过一个字节的限制 并按规定应用每个消息的开销。
// 获取limit长度以内【overhead计算在内】的任意节点可以发送的消息
// 每条消息overheadbyte的开销
func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	//overhead 消息头占用的大小
	q.mu.Lock()
	defer q.mu.Unlock()

	// 默认情况下的快速通道
	if q.lenLocked() == 0 {
		return nil
	}
	// 重试次数,根据集群规模调整重试次数
	transmitLimit := pkg.RetransmitLimit(q.RetransmitMult, q.NumNodes())

	var _ = MemberlistBroadcast{}
	var (
		bytesUsed int                 // 消息体已占用的字节数
		toSend    [][]byte            // 需要发送的消息
		reinsert  []*limitedBroadcast // 次数没超，需要重新添加到BTree的消息
		//	获取<=指定大小的消息;如果该消息重试次数>transmitLimit,删除;否则需要重新添加到BTree
	)

	// 返回消息树中重试次数的区间
	minTr, maxTr := q.getTransmitRange()
	for transmits := minTr; transmits <= maxTr; /*不自动前进*/ {
		free := int64(limit - bytesUsed - overhead) //消息体剩余的消息空间
		if free <= 0 {
			break //
		}
		// >=
		greaterOrEqual := &limitedBroadcast{
			transmits: transmits,
			msgLen:    free,
			id:        math.MaxInt64, // 消息进行比较，ID上限，为的是寻找transmits=transmits的消息
		}
		// <
		lessThan := &limitedBroadcast{
			transmits: transmits + 1,
			msgLen:    math.MaxInt64,
			id:        math.MaxInt64,
		}
		var keep *limitedBroadcast // 获取transmits次数下,消息长度远小于free的消息
		// 升序某个范围     a<= ? < b
		// 同一个重试次数下，数据是按照从最多到最小排布的
		q.tq.AscendRange(greaterOrEqual, lessThan, func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)
			// 检查这是否在我们的范围内
			if int64(len(cur.B.Message())) > free {
				//获取消息长度> free的消息
				return true
			}
			keep = cur
			return false
		})
		if keep == nil {
			// 该transmits中不再有适当大小的消息。
			transmits++
			continue
		}

		msg := keep.B.Message()

		// 添加到切片中，以便发送
		bytesUsed += overhead + len(msg)
		toSend = append(toSend, msg)
		// 从BTree中删除该消息
		q.DeleteItem(keep)

		// 检查我们是否应该停止传输
		// 可能因集群节点的变更，transmitLimit变小了，但是BTree中存在超过了该值的消息
		if keep.transmits+1 >= transmitLimit {
			// 因为次数是从0开始的
			// 重试次数，超过了限制
			keep.B.Finished()
		} else {
			// We need to bump this item down to another transmit tier, but
			// because it would be in the same direction that we're walking the
			// tiers, we will have to delay the reinsertion until we are
			// finished our search. Otherwise we'll possibly re-add the message
			// when we ascend to the next tier.
			//如果消息重试次数，没有超过的话，还需要继续添加到BTree中，重新发送
			keep.transmits++
			reinsert = append(reinsert, keep)
		}
	}

	for _, cur := range reinsert {
		q.addItem(cur)
	}

	return toSend
}

// 返回消息树中重试次数的区间
// 因为消息按照重试次数进行了排序
func (q *TransmitLimitedQueue) getTransmitRange() (minTransmit, maxTransmit int) {
	if q.lenLocked() == 0 {
		return 0, 0
	}
	minItem, maxItem := q.tq.Min(), q.tq.Max()
	if minItem == nil || maxItem == nil {
		return 0, 0
	}

	min := minItem.(*limitedBroadcast).transmits
	max := maxItem.(*limitedBroadcast).transmits

	return min, max
}

// DeleteItem 删除给定的项目。你必须已经持有该mutex。
func (q *TransmitLimitedQueue) DeleteItem(cur *limitedBroadcast) {
	_ = q.tq.Delete(cur)
	if cur.name != "" {
		delete(q.tm, cur.name)
	}

	if q.tq.Len() == 0 {
		// 在闲暇时，没有理由让idGen无限期地继续下去。
		q.idGen = 0
	}
}

// addItem 将给定的项目添加到整个数据结构中。你必须已经持有该mutex。
func (q *TransmitLimitedQueue) addItem(cur *limitedBroadcast) {
	_ = q.tq.ReplaceOrInsert(cur) // 替换或插入,返回存在的或nil
	if cur.name != "" {
		q.tm[cur.name] = cur
	}
}
