package memberlist

import (
	"github.com/hashicorp/memberlist/pkg"
	"net"
)
// ----------------------------------------- OK -------------------------------------------------

// PacketHandler 从listener解耦出来,避免阻塞 ,导致ping\ack延迟
func (m *Members) PacketHandler() {
	for {
		select {
		case <-m.HandoffCh: // 用户消息ch,当收到UserMsg时激活
			for {
				msg, ok := m.getNextMessage() // 从队列里拿,
				if !ok {
					break
				}
				msgType := msg.msgType
				buf := msg.buf
				from := msg.from
				switch msgType {
				case SuspectMsg: // ✅
					m.handleSuspect(buf, from)
				case AliveMsg: // ✅ 收到某个节点存活的消息
					m.handleAlive(buf, from)
				case DeadMsg: // ✅
					m.handleDead(buf, from)
				case UserMsg: // ✅
					m.handleUser(buf, from)
				default:
					m.Logger.Printf("[错误] memberlist: 消息类型不支持 (%d) 不支持 %s (packet handler)", msgType, pkg.LogAddress(from))
				}
			}
		case <-m.ShutdownCh:
			return
		}
	}
}

// OK
func (m *Members) handleSuspect(buf []byte, from net.Addr) {
	var sus Suspect
	if err := Decode(buf, &sus); err != nil {
		m.Logger.Printf("[错误] memberlist: 解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.SuspectNode(&sus)
}

// OK
func (m *Members) handleAlive(buf []byte, from net.Addr) {
	if err := m.ensureCanConnect(from); err != nil {
		m.Logger.Printf("[DEBUG] memberlist: 被封锁的活着的消息: %s %s", err, pkg.LogAddress(from))
		return
	}
	var live Alive
	if err := Decode(buf, &live); err != nil {
		m.Logger.Printf("[错误] memberlist:解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	if m.Config.IPMustBeChecked() {
		innerIP := net.IP(live.Addr)
		if innerIP != nil {
			if err := m.Config.IPAllowed(innerIP); err != nil {
				m.Logger.Printf("[DEBUG] memberlist: 封锁的活着的消息 %s  : %s %s", innerIP.String(), err, pkg.LogAddress(from))
				return
			}
		}
	}
	m.AliveNode(&live, nil, false)
}

// OK
func (m *Members) handleDead(buf []byte, from net.Addr) {
	var d Dead
	if err := Decode(buf, &d); err != nil {
		m.Logger.Printf("[错误] memberlist:解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.DeadNode(&d)
}

// handleUser 用来通知通道进入的用户数据。
func (m *Members) handleUser(buf []byte, from net.Addr) {
	_ = m.SendUserMsg
	_ = m.SendToAddress
	_ = m.SendBestEffort
	_ = m.SendToUDP
	//_ = m.Config.Delegate.GetBroadcasts

	d := m.Config.Delegate
	if d != nil {
		d.NotifyMsg(buf)
	}
}
