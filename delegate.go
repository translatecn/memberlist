package memberlist

import "time"

// Delegate is the interface that clients must implement if they want to hook
// into the gossip layer of Members. All the methods must be thread-safe,
// as they can and generally will be called concurrently.
type Delegate interface {
	// NodeMeta 用于在广播Alive消息时检索关于当前节点的元数据。它的长度被限制在给定的字节大小。这个元数据可以在Node结构中找到。
	NodeMeta(limit int) []byte

	// NotifyMsg is called when a user-data message is received.
	// Care should be taken that this method does not block, since doing
	// so would block the entire UDP packet receive loop. Additionally, the byte
	// slice may be modified after the call returns, so it should be copied if needed
	NotifyMsg([]byte)

	// GetBroadcasts is called when user data messages can be broadcast.
	// It can return a list of buffers to send. Each buffer should assume an
	// overhead as provided with a limit on the total byte size allowed.
	// The total byte size of the resulting data to send must not exceed
	// the limit. Care should be taken that this method does not block,
	// since doing so would block the entire UDP packet receive loop.
	GetBroadcasts(overhead, limit int) [][]byte

	// LocalState is used for a TCP Push/Pull. This is sent to
	// the remote side in addition to the membership information. Any
	// data can be sent here. See MergeRemoteState as well. The `join`
	// boolean indicates this is for a join instead of a push/pull.
	LocalState(join bool) []byte

	// MergeRemoteState is invoked after a TCP Push/Pull. This is the
	// state received from the remote side and is the result of the
	// remote side's LocalState call. The 'join'
	// boolean indicates this is for a join instead of a push/pull.
	MergeRemoteState(buf []byte, join bool)
}

type MergeDelegate interface {
	// NotifyMerge 远端节点合并到本地时,触发
	NotifyMerge(peers []*Node) error
}

// ConflictDelegate is a used to inform a client that
// a node has attempted to join which would result in a
// name conflict. This happens if two clients are configured
// with the same name but different Addresses.
type ConflictDelegate interface {
	// NotifyConflict is invoked when a name conflict is detected
	NotifyConflict(existing, other *Node)
}

// PingDelegate is used to notify an observer how long it took for a Ping message to
// complete a round trip.  It can also be used for writing arbitrary byte slices
// into ack messages. Note that in order to be meaningful for RTT estimates, this
// delegate does not apply to indirect pings, nor fallback pings sent over TCP.
type PingDelegate interface {
	// AckPayload 在发送ack时被调用；返回的字节将被附加到ack中。
	AckPayload() []byte
	// NotifyPingComplete  NotifyPing is invoked when an ack for a Ping is received
	NotifyPingComplete(other *Node, rtt time.Duration, payload []byte)
}

// AliveDelegate 是用来让客户端参与处理一个节点的 "Alive"消息。通过一个活着的消息更新该节点的状态。
type AliveDelegate interface {
	// NotifyAlive 从网络中接收到了某个节点的Alive信息
	NotifyAlive(peer *Node) error
}

// EventDelegate is a simpler delegate that is used only to receive
// notifications about members joining and leaving. The methods in this
// delegate may be called by multiple goroutines, but never concurrently.
// This allows you to reason about ordering.
type EventDelegate interface {
	// NotifyJoin is invoked when a node is detected to have joined.
	// The Node argument must not be modified.
	NotifyJoin(*Node)

	// NotifyLeave is invoked when a node is detected to have left.
	// The Node argument must not be modified.
	NotifyLeave(*Node)

	// NotifyUpdate is invoked when a node is detected to have
	// updated, usually involving the meta data. The Node argument
	// must not be modified.
	NotifyUpdate(*Node)
}

// ChannelEventDelegate is used to enable an application to receive
// events about joins and leaves over a channel instead of a direct
// function call.
//
// Care must be taken that events are processed in a timely manner from
// the channel, since this delegate will block until an event can be sent.
type ChannelEventDelegate struct {
	Ch chan<- NodeEvent
}

// NodeEventType are the types of events that can be sent from the
// ChannelEventDelegate.
type NodeEventType int

const (
	NodeJoin NodeEventType = iota
	NodeLeave
	NodeUpdate
)

// NodeEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified. It is passed
// as a pointer to avoid unnecessary copies. If you wish to modify the node,
// make a copy first.
type NodeEvent struct {
	Event NodeEventType
	Node  *Node
}

func (c *ChannelEventDelegate) NotifyJoin(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeJoin, &node}
}

func (c *ChannelEventDelegate) NotifyLeave(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeLeave, &node}
}

func (c *ChannelEventDelegate) NotifyUpdate(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeUpdate, &node}
}
