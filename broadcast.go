package memberlist

import "github.com/hashicorp/memberlist/queue_broadcast"

/*
The broadcast mechanism works by maintaining a sorted list of messages to be
sent out. When a message is to be broadcast, the retransmit count
is set to zero and appended to the queue_broadcast. The retransmit count serves
as the "priority", ensuring that newer messages get sent first. Once
a message hits the retransmit limit, it is removed from the queue_broadcast.

Additionally, older entries can be invalidated by new messages that
are contradictory. For example, if we send "{suspect M1 inc: 1},
then a following {alive M1 inc: 2} will invalidate that message
*/

// encodeBroadcast encodes a message and enqueues it for broadcast. Fails
// silently if there is an encoding error.
func (m *Members) encodeBroadcast(node string, msgType messageType, msg interface{}) {
	m.encodeBroadcastNotify(node, msgType, msg, nil)
}

// encodeBroadcastNotify 编码消息,并将其入队用于广播
func (m *Members) encodeBroadcastNotify(node string, msgType messageType, msg interface{}, notify chan struct{}) {
	buf, err := encode(msgType, msg)

	switch msg.(type) {
	case *alive:
	case *dead:
	case *suspect:
	default:

	}

	if err != nil {
		m.logger.Printf("[错误] memberlist: 编码用于广播的消息失败: %s", err)
	} else {
		m.queueBroadcast(node, buf.Bytes(), notify)
	}
}

// queueBroadcast 开始广播消息,它将被发送至配置的次数。该消息有可能被未来关于同一节点的消息所废止。
func (m *Members) queueBroadcast(node string, msg []byte, notify chan struct{}) {

	b := &queue_broadcast.MemberlistBroadcast{Node: node, Msg: msg, Notify: notify}
	m.broadcasts.QueueBroadcast(b)
}

// getBroadcasts is used to return a slice of broadcasts to send up to
// a maximum byte size, while imposing a per-broadcast overhead. This is used
// to fill a UDP packet with piggybacked data
func (m *Members) getBroadcasts(overhead, limit int) [][]byte {
	// Get memberlist messages first
	toSend := m.broadcasts.GetBroadcasts(overhead, limit)

	// Check if the user has anything to broadcast
	d := m.config.Delegate
	if d != nil {
		// Determine the bytes used already
		bytesUsed := 0
		for _, msg := range toSend {
			bytesUsed += len(msg) + overhead
		}

		// Check space remaining for user messages
		avail := limit - bytesUsed
		if avail > overhead+userMsgOverhead {
			userMsgs := d.GetBroadcasts(overhead+userMsgOverhead, avail)

			// Frame each user message
			for _, msg := range userMsgs {
				buf := make([]byte, 1, len(msg)+1)
				buf[0] = byte(userMsg)
				buf = append(buf, msg...)
				toSend = append(toSend, buf)
			}
		}
	}
	return toSend
}
