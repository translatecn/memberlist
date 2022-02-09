package queue_broadcast

import (
	"github.com/google/btree"
	"github.com/hashicorp/memberlist/pkg"
	"math"
	"sync"
)

// 被限制的广播
type limitedBroadcast struct {
	transmits int       // btree-key[0]: 尝试传输的次数
	msgLen    int64     // btree-key[1]: 消息长度len(b.Message())
	id        int64     // btree-key[2]: 提交时的唯一的递增标识
	B         Broadcast // 广播消息

	name string // set if Broadcast is a NamedBroadcast
}

// TransmitLimitedQueue
// 用于对消息进行队列，以便(通过gossip)向集群广播，但限制每条消息的传输数量。它还对传输计数较低的消息(因此是较新的消息)进行优先级排序。
type TransmitLimitedQueue struct {
	// NumNodes 返回集群中的节点数。这用于确定根据此日志计算的重传计数。
	NumNodes func() int
	// RetransmitMult 用于确定重传的最大次数的 重传系数。
	RetransmitMult int
	mu             sync.Mutex
	tq             *btree.BTree // stores *limitedBroadcast as btree.Item
	tm             map[string]*limitedBroadcast
	idGen          int64
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

// QueueBroadcast 广播消息入队
func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.queueBroadcast(b, 0)
}

// Less tests whether the current item is less than the given argument.
//
// This must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
// hold one of either a or b in the tree).
//
// default ordering is
// - [transmits=0, ..., transmits=inf]
// - [transmits=0:len=999, ..., transmits=0:len=2, ...]
// - [transmits=0:len=999,id=999, ..., transmits=0:len=999:id=1, ...]
func (b *limitedBroadcast) Less(than btree.Item) bool {
	o := than.(*limitedBroadcast)
	if b.transmits < o.transmits {
		return true
	} else if b.transmits > o.transmits {
		return false
	}
	if b.msgLen > o.msgLen {
		return true
	} else if b.msgLen < o.msgLen {
		return false
	}
	return b.id > o.id
}

// OrderedView for testing; emits in transmit order if reverse=false
func (q *TransmitLimitedQueue) OrderedView(reverse bool) []*limitedBroadcast {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.lenLocked())
	q.walkReadOnlyLocked(reverse, func(cur *limitedBroadcast) bool {
		out = append(out, cur)
		return true
	})

	return out
}

// walkReadOnlyLocked calls f for each item in the queue_broadcast traversing it in
// natural order (by Less) when reverse=false and the opposite when true. You
// must hold the mutex.
//
// This method panics if you attempt to mutate the item during traversal.  The
// underlying btree should also not be mutated during traversal.
func (q *TransmitLimitedQueue) walkReadOnlyLocked(reverse bool, f func(*limitedBroadcast) bool) {
	if q.lenLocked() == 0 {
		return
	}

	iter := func(item btree.Item) bool {
		cur := item.(*limitedBroadcast)

		prevTransmits := cur.transmits
		prevMsgLen := cur.msgLen
		prevID := cur.id

		keepGoing := f(cur)

		if prevTransmits != cur.transmits || prevMsgLen != cur.msgLen || prevID != cur.id {
			panic("edited queue_broadcast while walking read only")
		}

		return keepGoing
	}

	if reverse {
		q.tq.Descend(iter) // end with transmit 0
	} else {
		q.tq.Ascend(iter) // start with transmit 0
	}
}

func (q *TransmitLimitedQueue) queueBroadcast(b Broadcast, initialTransmits int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.lazyInit()
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
		unique = true
	}

	// 检查这个消息是否使另一个消息无效。
	if lb.name != "" {
		if old, ok := q.tm[lb.name]; ok {
			old.B.Finished()
			q.deleteItem(old)
		}
	} else if !unique {
		// lb.name == "" && unique == false
		//
		var remove []*limitedBroadcast
		q.tq.Ascend(func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)

			// Special Broadcasts can only invalidate each other.
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
			q.deleteItem(cur)
		}
	}

	// Append to the relevant queue_broadcast.
	q.addItem(lb)
}

// deleteItem removes the given item from the overall datastructure. You
// must already hold the mutex.
func (q *TransmitLimitedQueue) deleteItem(cur *limitedBroadcast) {
	_ = q.tq.Delete(cur)
	if cur.name != "" {
		delete(q.tm, cur.name)
	}

	if q.tq.Len() == 0 {
		// At idle there's no reason to let the id generator keep going
		// indefinitely.
		q.idGen = 0
	}
}

// addItem adds the given item into the overall datastructure. You must already
// hold the mutex.
func (q *TransmitLimitedQueue) addItem(cur *limitedBroadcast) {
	_ = q.tq.ReplaceOrInsert(cur)
	if cur.name != "" {
		q.tm[cur.name] = cur
	}
}

// getTransmitRange returns a pair of min/max values for transmit values
// represented by the current queue_broadcast contents. Both values represent actual
// transmit values on the interval [0, len). You must already hold the mutex.
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

// GetBroadcasts is used to get a number of broadcasts, up to a byte limit
// and applying a per-message overhead as provided.
func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Fast path the default case
	if q.lenLocked() == 0 {
		return nil
	}

	transmitLimit := pkg.RetransmitLimit(q.RetransmitMult, q.NumNodes())

	var (
		bytesUsed int
		toSend    [][]byte
		reinsert  []*limitedBroadcast
	)

	// Visit fresher items first, but only look at stuff that will fit.
	// We'll go tier by tier, grabbing the largest items first.
	minTr, maxTr := q.getTransmitRange()
	for transmits := minTr; transmits <= maxTr; /*do not advance automatically*/ {
		free := int64(limit - bytesUsed - overhead)
		if free <= 0 {
			break // bail out early
		}

		// Search for the least element on a given tier (by transmit count) as
		// defined in the limitedBroadcast.Less function that will fit into our
		// remaining space.
		greaterOrEqual := &limitedBroadcast{
			transmits: transmits,
			msgLen:    free,
			id:        math.MaxInt64,
		}
		lessThan := &limitedBroadcast{
			transmits: transmits + 1,
			msgLen:    math.MaxInt64,
			id:        math.MaxInt64,
		}
		var keep *limitedBroadcast
		q.tq.AscendRange(greaterOrEqual, lessThan, func(item btree.Item) bool {
			cur := item.(*limitedBroadcast)
			// Check if this is within our limits
			if int64(len(cur.B.Message())) > free {
				// If this happens it's a bug in the datastructure or
				// surrounding use doing something like having len(Message())
				// change over time. There's enough going on here that it's
				// probably sane to just skip it and move on for now.
				return true
			}
			keep = cur
			return false
		})
		if keep == nil {
			// No more items of an appropriate size in the tier.
			transmits++
			continue
		}

		msg := keep.B.Message()

		// Add to slice to send
		bytesUsed += overhead + len(msg)
		toSend = append(toSend, msg)

		// Check if we should stop transmission
		q.deleteItem(keep)
		if keep.transmits+1 >= transmitLimit {
			keep.B.Finished()
		} else {
			// We need to bump this item down to another transmit tier, but
			// because it would be in the same direction that we're walking the
			// tiers, we will have to delay the reinsertion until we are
			// finished our search. Otherwise we'll possibly re-add the message
			// when we ascend to the next tier.
			keep.transmits++
			reinsert = append(reinsert, keep)
		}
	}

	for _, cur := range reinsert {
		q.addItem(cur)
	}

	return toSend
}

// NumQueued returns the number of queued messages
func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lenLocked()
}

// lenLocked returns the length of the overall queue_broadcast datastructure. You must
// hold the mutex.
func (q *TransmitLimitedQueue) lenLocked() int {
	if q.tq == nil {
		return 0
	}
	return q.tq.Len()
}

// Reset clears all the queued messages. Should only be used for tests.
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

// Prune will retain the maxRetain latest messages, and the rest
// will be discarded. This can be used to prevent unbounded queue_broadcast sizes
func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Do nothing if queue_broadcast size is less than the limit
	for q.tq.Len() > maxRetain {
		item := q.tq.Max()
		if item == nil {
			break
		}
		cur := item.(*limitedBroadcast)
		cur.B.Finished()
		q.deleteItem(cur)
	}
}
