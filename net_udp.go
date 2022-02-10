package memberlist

import (
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"hash/crc32"
	"net"
	"time"
)

// PacketListen 将包从传输中取出，并处理，server端
func (m *Members) PacketListen() {
	for {
		select {
		case packet := <-m.Transport.PacketCh(): // 直接消息传递; 读取来自其他节点的消息
			// 客户端发送
			//server  生成packet 往通道发消息
			//_ = m.Transport.(*NetTransport).RecIngestPacket //往通道发消息
			//_ = m.Transport.(*NetTransport).UdpListen // 往通道发消息
			m.HandleIngestPacket(packet.Buf, packet.From, packet.Timestamp)
		case <-m.ShutdownCh:
			return
		}
	}
}

// HandleIngestPacket 接收包,解密、校验
func (m *Members) HandleIngestPacket(buf []byte, from net.Addr, timestamp time.Time) {
	var (
		packetLabel string
		err         error
	)
	buf, packetLabel, err = RemoveLabelHeaderFromPacket(buf) // 移除标签头后的数据
	if err != nil {
		m.Logger.Printf("[错误] memberlist: %v %s", err, pkg.LogAddress(from))
		return
	}

	if m.Config.SkipInboundLabelCheck {
		if packetLabel != "" {
			m.Logger.Printf("[错误] memberlist:出乎意料的双数据包 标签头: %s", pkg.LogAddress(from))
			return
		}
		packetLabel = m.Config.Label
	}

	if m.Config.Label != packetLabel {
		m.Logger.Printf("[错误] memberlist: 丢弃具有不可接受的标签的数据包 %q: %s", packetLabel, pkg.LogAddress(from))
		return
	}

	if m.Config.EncryptionEnabled() {
		authData := []byte(packetLabel) // 盐
		plain, err := DecryptPayload(m.Config.Keyring.GetKeys(), buf, authData)
		if err != nil {
			if !m.Config.GossipVerifyIncoming {
				// 将信息视为明文
				plain = buf
			} else {
				m.Logger.Printf("[错误] memberlist: 解密失败: %v %s", err, pkg.LogAddress(from))
				return
			}
		}

		// 继续处理明文缓冲区
		buf = plain
	}

	// 看看是否包括一个校验和，以验证信息的内容
	if len(buf) >= 5 && MessageType(buf[0]) == HasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			m.Logger.Printf("[WARN] memberlist: 发现UDP数据包的校验值无效: %x, %x", crc, expected)
			return
		}
		m.HandleCommand(buf[5:], from, timestamp)
	} else {
		m.HandleCommand(buf, from, timestamp)
	}
}

// HandleCommand 处理消息
func (m *Members) HandleCommand(buf []byte, from net.Addr, timestamp time.Time) {
	if len(buf) < 1 {
		m.Logger.Printf("[错误] memberlist: 缺少信息类型的字节 %s", pkg.LogAddress(from))
		return
	}

	msgType := MessageType(buf[0])
	buf = buf[1:]

	switch msgType {
	case CompoundMsg: // ✅组合消息
		m.handleCompound(buf, from, timestamp)
	case CompressMsg: // ✅ 压缩消息
		m.handleCompressed(buf, from, timestamp)
	case PingMsg: // 接收到PING  ✅
		m.handlePing(buf, from)
	case IndirectPingMsg:
		m.handleIndirectPing(buf, from)
	case AckRespMsg: // ✅ Ping确认消息
		m.handleAck(buf, from, timestamp)
	case NAckRespMsg:
		m.handleNack(buf, from)
	case SuspectMsg: // ✅ 质疑消息
		fallthrough
	case AliveMsg: // ✅ 存活消息
		fallthrough
	case DeadMsg: // ✅ 死亡消息
		fallthrough
	case UserMsg: // ✅ 用户消息
		// 优先Alive
		queue := m.LowPriorityMsgQueue
		if msgType == AliveMsg {
			queue = m.HighPriorityMsgQueue
		}

		// 检查是否有溢出，如果没有满，则进行追加。
		m.msgQueueLock.Lock()
		if queue.Len() >= m.Config.HandoffQueueDepth {
			m.Logger.Printf("[WARN] memberlist: 队列溢出 (%d) %s", msgType, pkg.LogAddress(from))
		} else {
			queue.PushBack(msgHandoff{msgType, buf, from}) // 移交消息
		}
		m.msgQueueLock.Unlock()

		// 通知有待处理的信息
		select {
		case m.HandoffCh <- struct{}{}:
		default:
		}

	default:
		m.Logger.Printf("[错误] memberlist: 消息类型不支持 (%d) %s", msgType, pkg.LogAddress(from))
	}
}

func (m *Members) handleNack(buf []byte, from net.Addr) {
	var nack NAckResp
	if err := Decode(buf, &nack); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode nack response: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.InvokeNAckHandler(nack)
}

// handleCompressed 加压 消息
func (m *Members) handleCompressed(buf []byte, from net.Addr, timestamp time.Time) {
	payload, err := DeCompressPayload(buf)
	if err != nil {
		m.Logger.Printf("[错误] memberlist:解压失败: %v %s", err, pkg.LogAddress(from))
		return
	}
	//递归调用
	m.HandleCommand(payload, from, timestamp)
}

//处理复合消息
func (m *Members) handleCompound(buf []byte, from net.Addr, timestamp time.Time) {
	trunc, parts, err := DecodeCompoundMessage(buf)
	// trunc 有几部分没有数据
	if err != nil {
		m.Logger.Printf("[错误] memberlist:消息解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}

	// Log any truncation
	if trunc > 0 {
		m.Logger.Printf("[WARN] memberlist: 消息意外截断 %d     %s", trunc, pkg.LogAddress(from))
	}

	for _, part := range parts {
		// 每一部分都有消息类型
		m.HandleCommand(part, from, timestamp)
	}
}

func (m *Members) handleIndirectPing(buf []byte, from net.Addr) {
	var ind IndirectPingReq
	if err := Decode(buf, &ind); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode indirect Ping request: %s %s", err, pkg.LogAddress(from))
		return
	}

	// For proto versions < 2, there is no Port provided. Mask old
	// behavior by using the configured Port.
	if m.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(m.Config.BindPort)
	}

	// Send a Ping to the correct host.
	localSeqNo := m.NextSeqNo()
	selfAddr, selfPort := m.getAdvertise()
	ping := Ping{
		SeqNo: localSeqNo,
		Node:  ind.Node,
		// The outbound message is Addressed FROM us.
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}

	// Forward the ack back to the requestor. If the request encodes an origin
	// use that otherwise assume that the other end of the UDP socket is
	// usable.
	indAddr := ""
	if len(ind.SourceAddr) > 0 && ind.SourcePort > 0 {
		indAddr = pkg.JoinHostPort(net.IP(ind.SourceAddr).String(), ind.SourcePort)
	} else {
		indAddr = from.String()
	}

	// Setup a response handler to relay the ack
	cancelCh := make(chan struct{})
	respHandler := func(payload []byte, timestamp time.Time) {
		// Try to prevent the nack if we've caught it in time.
		close(cancelCh)

		ack := AckResp{ind.SeqNo, nil}
		a := Address{
			Addr: indAddr,
			Name: ind.SourceNode,
		}
		if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to forward ack: %s %s", err, pkg.LogStringAddress(indAddr))
		}
	}
	m.SetAckHandler(localSeqNo, respHandler, m.Config.ProbeTimeout)

	Addr := pkg.JoinHostPort(net.IP(ind.Target).String(), ind.Port)
	a := Address{
		Addr: Addr,
		Name: ind.Node,
	}
	if err := m.encodeAndSendMsg(a, PingMsg, &ping); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to send indirect Ping: %s %s", err, pkg.LogStringAddress(indAddr))
	}

	// Setup a timer to fire off a nack if no ack is seen in time.
	if ind.Nack {
		go func() {
			select {
			case <-cancelCh:
				return
			case <-time.After(m.Config.ProbeTimeout):
				nack := NAckResp{ind.SeqNo}
				a := Address{
					Addr: indAddr,
					Name: ind.SourceNode,
				}
				if err := m.encodeAndSendMsg(a, NAckRespMsg, &nack); err != nil {
					m.Logger.Printf("[错误] memberlist: Failed to send nack: %s %s", err, pkg.LogStringAddress(indAddr))
				}
			}
		}()
	}
}

// 确认PING 响应
func (m *Members) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack AckResp
	if err := Decode(buf, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist: 解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.InvokeAckHandler(ack, timestamp)
}

// InvokeAckHandler 如果有相关的ack处理程序，则调用一个ack处理程序，并立即收获处理程序。
func (m *Members) InvokeAckHandler(ack AckResp, timestamp time.Time) {
	m.AckLock.Lock()
	_ = m.SetProbeChannels // 将待确认加入AckHandlers
	ah, ok := m.AckHandlers[ack.SeqNo]
	delete(m.AckHandlers, ack.SeqNo)
	m.AckLock.Unlock()
	if !ok {
		return
	}
	ah.timer.Stop()
	ah.ackFn(ack.Payload, timestamp)
}

func (m *Members) InvokeNAckHandler(nack NAckResp) {
	m.AckLock.Lock()
	ah, ok := m.AckHandlers[nack.SeqNo]
	m.AckLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	ah.nackFn()
}

// UDP 响应需要直接发包
func (m *Members) handlePing(buf []byte, from net.Addr) {
	var p Ping
	if err := Decode(buf, &p); err != nil {
		m.Logger.Printf("[错误] memberlist:解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	// 如果提供了节点，请核实它是为我们准备的
	if p.Node != "" && p.Node != m.Config.Name {
		m.Logger.Printf("[WARN] memberlist: 得到了意外节点的Ping '%s' %s", p.Node, pkg.LogAddress(from))
		return
	}
	var ack AckResp
	ack.SeqNo = p.SeqNo
	if m.Config.Ping != nil {
		ack.Payload = m.Config.Ping.AckPayload()
	}

	Addr := ""
	if len(p.SourceAddr) > 0 && p.SourcePort > 0 {
		Addr = pkg.JoinHostPort(net.IP(p.SourceAddr).String(), p.SourcePort)
	} else {
		Addr = from.String()
	}
	fmt.Printf("PING---->%+v\n", p.PingCopy(net.IP(p.SourceAddr).String()))

	a := Address{
		Addr: Addr,
		Name: p.SourceNode,
	}
	if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist:回复ACK失败: %s %s", err, pkg.LogAddress(from))
	}
}

// ----------------------------------------- CLIENT -------------------------------------------------

// Gossip 定时广播到随机的几台机器
func (m *Members) Gossip() {

	m.NodeLock.RLock()
	//随机获取一台机器
	kNodes := KRandomNodes(m.Config.GossipNodes, m.Nodes, func(n *NodeState) bool {
		if n.Name == m.Config.Name {
			// 忽略自己
			return true
		}

		switch n.State {
		case StateAlive, StateSuspect:
			return false

		case StateDead:
			return time.Since(n.StateChange) > m.Config.GossipToTheDeadTime

		default:
			return true
		}
	})
	m.NodeLock.RUnlock()

	// 计算可用的字节数
	// 1400 - 2 - label信息 - 加密信息
	bytesAvail := m.Config.UDPBufferSize - CompoundHeaderOverhead - LabelOverhead(m.Config.Label)
	if m.Config.EncryptionEnabled() {
		bytesAvail -= encryptOverhead(m.EncryptionVersion())
	}

	// a 1,2,3,4
	// 1 --> b
	// 2 --> c
	// 3 --> d
	for _, node := range kNodes {
		// 获取任何未完成的广播节目
		msgs := m.getBroadcasts(CompoundOverhead, bytesAvail)
		if len(msgs) == 0 {
			return
		}

		Addr := node.Address()
		if len(msgs) == 1 {
			// 按原样发送单一信息
			if err := m.RawSendMsgPacket(node.FullAddress(), &node, msgs[0]); err != nil {
				m.Logger.Printf("[错误] memberlist: gossip消息发送失败 %s: %s", Addr, err)
			}
		} else {
			// 否则将创建并发送一个或多个复合信息
			compounds := MakeCompoundMessages(msgs)
			for _, compound := range compounds {
				if err := m.RawSendMsgPacket(node.FullAddress(), &node, compound.Bytes()); err != nil {
					m.Logger.Printf("[错误] memberlist: gossip消息发送失败 %s: %s", Addr, err)
				}
			}
		}
	}
}

// ProbeNode 单个节点的故障检查。
func (m *Members) ProbeNode(node *NodeState) {

	// 我们使用我们的health awareness来扩展整个探测间隔，
	// 所以如果我们检测到问题，我们会放慢速度。
	// 调用我们的探测器可以处理我们运行超过基本间隔的情况，并会跳过错过的刻度。
	probeInterval := m.Awareness.ScaleTimeout(m.Config.ProbeInterval) // 根据健康度、设置探活时间

	selfAddr, selfPort := m.getAdvertise()
	ping := Ping{
		SeqNo:      m.NextSeqNo(),
		Node:       node.Name,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}
	ackCh := make(chan AckMessage, m.Config.IndirectChecks+1)
	nackCh := make(chan struct{}, m.Config.IndirectChecks+1)
	m.SetProbeChannels(ping.SeqNo, ackCh, nackCh, probeInterval)

	// Mark the sent time here, which should be after any pre-processing but
	// before system calls to do the actual send. This probably over-reports
	// a bit, but it's the best we can do. We had originally put this right
	// after the I/O, but that would sometimes give negative RTT measurements
	// which was not desirable.
	sent := time.Now()

	// Send a Ping to the node. If this node looks like it's Suspect or Dead,
	// also tack on a Suspect message so that it has a chance to Refute as
	// soon as possible.
	Deadline := sent.Add(probeInterval)
	Addr := node.Address()

	// Arrange for our self-awareness to get updated.
	var awarenessDelta int
	defer func() {
		m.Awareness.ApplyDelta(awarenessDelta)
	}()
	if node.State == StateAlive {
		if err := m.encodeAndSendMsg(node.FullAddress(), PingMsg, &ping); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to send Ping: %s", err)
			if FailedRemote(err) {
				goto HANDLE_REMOTE_FAILURE
			} else {
				return
			}
		}
	} else {
		var msgs [][]byte
		if buf, err := Encode(PingMsg, &ping); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to Encode Ping message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}
		s := Suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.Config.Name}
		if buf, err := Encode(SuspectMsg, &s); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to Encode Suspect message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}

		compound := MakeCompoundMessage(msgs)
		if err := m.RawSendMsgPacket(node.FullAddress(), &node.Node, compound.Bytes()); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to send compound Ping and Suspect message to %s: %s", Addr, err)
			if FailedRemote(err) {
				goto HANDLE_REMOTE_FAILURE
			} else {
				return
			}
		}
	}

	// Arrange for our self-awareness to get updated. At this point we've
	// sent the Ping, so any return statement means the probe succeeded
	// which will improve our health until we get to the failure scenarios
	// at the end of this function, which will alter this delta variable
	// accordingly.
	awarenessDelta = -1

	// Wait for response or round-trip-time.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			if m.Config.Ping != nil {
				rtt := v.Timestamp.Sub(sent)
				m.Config.Ping.NotifyPingComplete(&node.Node, rtt, v.Payload)
			}
			return
		}

		// As an edge case, if we get a timeout, we need to re-enqueue it
		// here to break out of the select below.
		if v.Complete == false {
			ackCh <- v
		}
	case <-time.After(m.Config.ProbeTimeout):
		// Note that we don't scale this timeout based on awareness and
		// the health score. That's because we don't really expect waiting
		// longer to help get UDP through. Since health does extend the
		// probe interval it will give the TCP fallback more time, which
		// is more active in dealing with lost packets, and it gives more
		// time to wait for indirect acks/nacks.
		m.Logger.Printf("[DEBUG] memberlist: Failed Ping: %s (timeout reached)", node.Name)
	}

HANDLE_REMOTE_FAILURE:
	// Get some random live Nodes.
	m.NodeLock.RLock()
	kNodes := KRandomNodes(m.Config.IndirectChecks, m.Nodes, func(n *NodeState) bool {
		return n.Name == m.Config.Name ||
			n.Name == node.Name ||
			n.State != StateAlive
	})
	m.NodeLock.RUnlock()

	// Attempt an indirect Ping.
	expectedNacks := 0
	selfAddr, selfPort = m.getAdvertise()
	ind := IndirectPingReq{
		SeqNo:      ping.SeqNo,
		Target:     node.Addr,
		Port:       node.Port,
		Node:       node.Name,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}
	for _, peer := range kNodes {
		// We only expect nack to be sent from peers who understand
		// version 4 of the protocol.
		if ind.Nack = peer.PMax >= 4; ind.Nack {
			expectedNacks++
		}

		if err := m.encodeAndSendMsg(peer.FullAddress(), IndirectPingMsg, &ind); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to send indirect Ping: %s", err)
		}
	}

	// Also make an attempt to contact the node directly over TCP. This
	// helps prevent confused clients who get isolated from UDP traffic
	// but can still speak TCP (which also means they can possibly report
	// misinformation to other Nodes via anti-entropy), avoiding flapping in
	// the cluster.
	//
	// This is a little unusual because we will attempt a TCP Ping to any
	// member who understands version 3 of the protocol, regardless of
	// which 协议版本 we are speaking. That's why we've included a
	// Config option to turn this off if desired.
	fallbackCh := make(chan bool, 1)

	disableTcpPings := m.Config.DisableTcpPings ||
		(m.Config.DisableTcpPingsForNode != nil && m.Config.DisableTcpPingsForNode(node.Name))
	if (!disableTcpPings) && (node.PMax >= 3) {
		go func() {
			defer close(fallbackCh)
			didContact, err := m.SendPingAndWaitForAck(node.FullAddress(), ping, Deadline)
			if err != nil {
				var to string
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					to = fmt.Sprintf("timeout %s: ", probeInterval)
				}
				m.Logger.Printf("[错误] memberlist: Failed fallback Ping: %s%s", to, err)
			} else {
				fallbackCh <- didContact
			}
		}()
	} else {
		close(fallbackCh)
	}

	// Wait for the acks or timeout. Note that we don't check the fallback
	// channel here because we want to issue a warning below if that's the
	// *only* way we hear back from the peer, so we have to let this time
	// out first to allow the normal UDP-based acks to come in.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			return
		}
	}

	// Finally, poll the fallback channel. The timeouts are set such that
	// the channel will have something or be closed without having to wait
	// any additional time here.
	for didContact := range fallbackCh {
		if didContact {
			m.Logger.Printf("[WARN] memberlist: Was able to connect to %s but other probes failed, network may be misconfigured", node.Name)
			return
		}
	}

	// Update our self-awareness based on the results of this failed probe.
	// If we don't have peers who will send nacks then we penalize for any
	// failed probe as a simple health metric. If we do have peers to nack
	// verify, then we can use that as a more sophisticated measure of self-
	// health because we assume them to be working, and they can help us
	// decide if the probed node was really Dead or if it was something wrong
	// with ourselves.
	awarenessDelta = 0
	if expectedNacks > 0 {
		if nackCount := len(nackCh); nackCount < expectedNacks {
			awarenessDelta += (expectedNacks - nackCount)
		}
	} else {
		awarenessDelta += 1
	}

	// No acks received from target, Suspect it as failed.
	m.Logger.Printf("[INFO] memberlist: Suspect %s has failed, no acks received", node.Name)
	s := Suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.Config.Name}
	m.SuspectNode(&s)
}

// Ping 发送ping到指定节点
func (m *Members) Ping(node string, Addr net.Addr) (time.Duration, error) {
	// ls-2018.local  ,直接测的本机          127.0.0.1:8000
	// 准备一个Ping消息并设置一个ack处理程序。
	selfAddr, selfPort := m.getAdvertise() // 10.10.16.207  8000
	ping := Ping{
		SeqNo:      m.NextSeqNo(),
		Node:       node,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}
	ackCh := make(chan AckMessage, m.Config.IndirectChecks+1)
	m.SetProbeChannels(ping.SeqNo, ackCh, nil, m.Config.ProbeInterval) // 设置ackFc NackFc 以及超时Fc

	a := Address{Addr: Addr.String(), Name: node}
	if err := m.encodeAndSendMsg(a, PingMsg, &ping); err != nil {
		return 0, err
	}

	// 在这里标记发送时间，这应该是在任何预处理和系统调用完成实际发送之后。
	// 这可能有点报告不足，但这是我们能做的最好的。
	sent := time.Now()

	select {
	case v := <-ackCh:
		_ = AckMessage{true, nil, time.Now()}
		if v.Complete == true {
			return v.Timestamp.Sub(sent), nil
		}
	case <-time.After(m.Config.ProbeTimeout):
		// Timeout, return an error below.
	}

	m.Logger.Printf("[DEBUG] memberlist: udp PING 失败: %v (timeout reached)", node)
	return 0, NoPingResponseError{ping.Node}
}
