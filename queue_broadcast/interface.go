package queue_broadcast

import "github.com/google/btree"

// Broadcast 通过gossip协议发送到集群中成员
type Broadcast interface {
	// Invalidates 检查 当前广播消息 是否使先前的广播无效
	Invalidates(b Broadcast) bool

	// Message 返回消息体
	Message() []byte

	// Finished 当信息不再被广播时被调用，无论是由于无效还是由于 达到发送限制
	Finished()
}
type NamedBroadcast interface {
	Broadcast
	// Name 广播消息的唯一标识符 ,消息发送方的主机名
	Name() string
}
type UniqueBroadcast interface {
	Broadcast
	UniqueBroadcast()
}

// 被限制的广播
type limitedBroadcast struct {
	transmits int       // btree-key[0]: 尝试传输的次数
	msgLen    int64     // btree-key[1]: 消息长度len(b.Message())
	id        int64     // btree-key[2]: 提交时的唯一的递增标识
	B         Broadcast // 广播消息

	name string // set if Broadcast is a NamedBroadcast
}

// Less 比较b 是不是小于 than [[1,18],[1,16],[2,1],[2,1]]
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

// --------------------------------------------------------------------------------

type MemberlistBroadcast struct {
	Node   string
	Msg    []byte
	Notify chan struct{}
}

// Invalidates 类型校验、验证消息是不是来自同一台机器
func (b *MemberlistBroadcast) Invalidates(other Broadcast) bool {
	mb, ok := other.(*MemberlistBroadcast)
	if !ok {
		return false
	}

	return b.Node == mb.Node
}

// Name ok
func (b *MemberlistBroadcast) Name() string {
	return b.Node
}

// Message OK
func (b *MemberlistBroadcast) Message() []byte {
	return b.Msg
}

// Finished ok
func (b *MemberlistBroadcast) Finished() {
	// case <-notifyCh:
	// case <-m.LeaveBroadcast:
	select {
	case b.Notify <- struct{}{}:
	default:
	}
}
