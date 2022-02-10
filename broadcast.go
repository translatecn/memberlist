package memberlist

import "github.com/hashicorp/memberlist/queue_broadcast"

/*
The broadcast mechanism works by maintaining a sorted list of messages to be
sent out. When a message is to be broadcast, the retransmit count
is set to zero and appended to the queue_broadcast. The retransmit count serves
as the "priority", ensuring that newer messages get sent first. Once
a message hits the retransmit limit, it is removed from the queue_broadcast.

Additionally, older entries can be invalidated by new messages that
are contradictory. For example, if we send "{Suspect M1 inc: 1},
then a following {Alive M1 inc: 2} will invalidate that message
*/

// EncodeBroadcast 编码消息,并将其入队用于广播
func (m *Members) EncodeBroadcast(node string, msgType MessageType, msg interface{}) {
	m.EncodeBroadcastNotify(node, msgType, msg, nil)
}

// EncodeBroadcastNotify 编码消息,并将其入队用于广播
func (m *Members) EncodeBroadcastNotify(node string, msgType MessageType, msg interface{}, notify chan struct{}) {
	buf, err := Encode(msgType, msg)

	switch msg.(type) {
	case *Alive:
	case *Dead:
	case *Suspect:
	default:

	}

	if err != nil {
		m.Logger.Printf("[错误] memberlist: 编码用于广播的消息失败: %s", err)
	} else {
		m.queueBroadcast(node, buf.Bytes(), notify)
	}
}

// queueBroadcast 开始广播消息,它将被发送至配置的次数。该消息有可能被未来关于同一节点的消息所废止。
func (m *Members) queueBroadcast(node string, msg []byte, notify chan struct{}) {
	b := &queue_broadcast.MemberlistBroadcast{Node: node, Msg: msg, Notify: notify}
	m.Broadcasts.QueueBroadcast(b)
}

// getBroadcasts
// 是用来返回一个广播片，以发送最大的字节大小，同时施加每个广播的开销。这被用来在UDP数据包中填充捎带的数据
func (m *Members) getBroadcasts(overhead, limit int) [][]byte {
	toSend := m.Broadcasts.GetBroadcasts(overhead, limit)

	// 检查用户是否有东西要广播
	d := m.Config.Delegate
	if d != nil {
		// Determine the bytes used already
		bytesUsed := 0
		for _, msg := range toSend {
			bytesUsed += len(msg) + overhead
		}

		// Check space remaining for user messages
		avail := limit - bytesUsed
		if avail > overhead+UserMsgOverhead {
			UserMsgs := d.GetBroadcasts(overhead+UserMsgOverhead, avail)

			// Frame each user message
			for _, msg := range UserMsgs {
				buf := make([]byte, 1, len(msg)+1)
				buf[0] = byte(UserMsg)
				buf = append(buf, msg...)
				toSend = append(toSend, buf)
			}
		}
	}
	return toSend
}
