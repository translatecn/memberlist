package memberlist

import (
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"hash/crc32"
	"net"
	"time"
)

// ----------------------------------------- SERVER -------------------------------------------------

//OK
func (m *Members) handleNack(buf []byte, from net.Addr) {
	var nack NAckResp
	if err := Decode(buf, &nack); err != nil {
		m.Logger.Printf("[错误] memberlist:解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.InvokeNAckHandler(nack)
}

// InvokeNAckHandler OK
func (m *Members) InvokeNAckHandler(nack NAckResp) {
	m.AckLock.Lock()
	ah, ok := m.AckHandlers[nack.SeqNo]
	m.AckLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	_ = m.SetProbeChannels
	_ = m.ProbeNode
	ah.nackFn()
}

// OK
func (m *Members) handleIndirectPing(buf []byte, from net.Addr) {
	// 发送代码搜
	//  m.encodeAndSendMsg(peer.FullAddress(), IndirectPingMsg
	var ind IndirectPingReq
	if err := Decode(buf, &ind); err != nil {
		m.Logger.Printf("[错误] memberlist:解码间接PING失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	if m.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(m.Config.BindPort)
	}

	// 项目标节点发送ping
	localSeqNo := m.NextSeqNo()
	selfAddr, selfPort := m.getAdvertise()
	ping := Ping{
		SeqNo: localSeqNo,
		Node:  ind.Node,
		// 呼出的信息是从我们这里发出的。
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}

	// 将ack转发回请求者。
	// 如果请求编码为一个原点，则使用该原点，否则假定UDP套接字的另一端是可用的。
	indAddr := ""
	if len(ind.SourceAddr) > 0 && ind.SourcePort > 0 {
		indAddr = pkg.JoinHostPort(net.IP(ind.SourceAddr).String(), ind.SourcePort)
	} else {
		indAddr = from.String()
	}

	// 设置一个响应处理程序来转发Ack
	cancelCh := make(chan struct{})
	_ = m.InvokeAckHandler
	ackFc := func(payload []byte, timestamp time.Time) {
		close(cancelCh)

		ack := AckResp{ind.SeqNo, nil}
		a := pkg.Address{ // 发送方
			Addr: indAddr,
			Name: ind.SourceNode,
		}
		if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
			m.Logger.Printf("[错误] memberlist:想发送方返回ACK失败: %s %s", err, pkg.LogStringAddress(indAddr))
		}
	}
	m.SetAckHandler(localSeqNo, ackFc, m.Config.ProbeTimeout)

	Addr := pkg.JoinHostPort(net.IP(ind.Target).String(), ind.Port)
	a := pkg.Address{
		Addr: Addr, // 探活地址
		Name: ind.Node,
	}
	if err := m.encodeAndSendMsg(a, PingMsg, &ping); err != nil {
		m.Logger.Printf("[错误] memberlist: 发送间接PING失败: %s %s", err, pkg.LogStringAddress(indAddr))
	}

	// 设置一个定时器，如果没有及时看到ack，就发射一个nack。
	if ind.Nack { // 发送方需不要返回NACK信号
		go func() {
			select {
			case <-cancelCh: // 收到了目标节点返回的ACK消息，就会关闭 cancelCh
				return
			case <-time.After(m.Config.ProbeTimeout):
				nack := NAckResp{ind.SeqNo}
				a := pkg.Address{
					Addr: indAddr,
					Name: ind.SourceNode,
				} // 如果超时了，就像发送节点返回Nack 消息
				if err := m.encodeAndSendMsg(a, NAckRespMsg, &nack); err != nil {
					m.Logger.Printf("[错误] memberlist: 发送nack失败: %s %s", err, pkg.LogStringAddress(indAddr))
				}
			}
		}()
	}
}

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
		//发送方
		_ = m.handleIndirectPing
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

// 给PING的发送方，发出确认PING 响应
func (m *Members) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack AckResp
	if err := Decode(buf, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist: 解码失败: %s %s", err, pkg.LogAddress(from))
		return
	}
	m.InvokeAckHandler(ack, timestamp)
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

	a := pkg.Address{
		Addr: Addr,
		Name: p.SourceNode,
	}
	if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist:回复ACK失败: %s %s", err, pkg.LogAddress(from))
	}
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

// ----------------------------------------- CLIENT -------------------------------------------------

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
	ackCh := make(chan AckMessage, m.Config.IndirectChecks+1) // 间接检查的次数+ 直接检查的次数
	nackCh := make(chan struct{}, m.Config.IndirectChecks+1)
	m.SetProbeChannels(ping.SeqNo, ackCh, nackCh, probeInterval)

	sent := time.Now()
	Deadline := sent.Add(probeInterval)
	Addr := node.Address()

	var awarenessDelta int // 警觉增量
	defer func() {
		m.Awareness.ApplyDelta(awarenessDelta)
	}()
	if node.State == StateAlive {
		if err := m.encodeAndSendMsg(node.FullAddress(), PingMsg, &ping); err != nil { // 有可能携带其他的广播消息
			m.Logger.Printf("[错误] memberlist: 发送PING失败: %s", err)
			if FailedRemote(err) { // 是不是服务崩了
				goto HandleRemoteFailure
			} else {
				// 其他错误
				return
			}
		}
	} else {
		// 节点不是StateAlive状态的前提下
		// 发送PING 、Suspect 组合消息
		var msgs [][]byte
		if buf, err := Encode(PingMsg, &ping); err != nil {
			m.Logger.Printf("[错误] memberlist:编码失败: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}
		s := Suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.Config.Name}
		if buf, err := Encode(SuspectMsg, &s); err != nil {
			m.Logger.Printf("[错误] memberlist:编码失败: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}

		compound := MakeCompoundMessage(msgs)
		if err := m.RawSendMsgPacket(node.FullAddress(), &node.Node, compound.Bytes()); err != nil {
			m.Logger.Printf("[错误] memberlist:发送compound Ping and Suspect message 失败 %s: %s", Addr, err)
			if FailedRemote(err) {
				goto HandleRemoteFailure
			} else {
				return
			}
		}
	}

	// 安排我们的自我警觉得到更新。在这一点上，我们已经发送了Ping，所以任何返回语句都意味着探测成功，这将改善我们的健康状况，
	// 直到我们在这个函数的结尾处遇到失败情况，这将相应地改变这个delta变量。
	awarenessDelta = -1

	// Wait for 响应 or 往返时间.
	select {
	case v := <-ackCh:
		_ = m.handleAck        // 接收
		_ = m.SetProbeChannels // 设置   成功发送TRUE，超时发送FALSE   超时时间 ProbeInterval
		if v.Complete == true {
			if m.Config.Ping != nil {
				rtt := v.Timestamp.Sub(sent) // 往返时间
				m.Config.Ping.NotifyPingComplete(&node.Node, rtt, v.Payload)
			}
			return
		}
		if v.Complete == false {
			// 无缺确保m.Config.ProbeInterval 与m.Config.ProbeTimeout 谁先到来,重新扔回channel
			ackCh <- v
		}
	case <-time.After(m.Config.ProbeTimeout): //
		// 请注意，我们没有根据警觉和健康评分来调整这个超时。这是因为我们并不指望等待的时间长能帮助UDP通过。
		// 由于健康状况确实延长了探测间隔，它将给TCP回退更多的时间，它在处理丢失的数据包时更加积极，而且它给了更多的时间来等待间接的acks/nacks。
		m.Logger.Printf("[DEBUG] memberlist: 失败 Ping: %s (timeout reached)", node.Name)
	}
	// 探测失败
HandleRemoteFailure:
	//获取几个随机的存活结点
	m.NodeLock.RLock()
	kNodes := KRandomNodes(m.Config.IndirectChecks, m.Nodes, func(n *NodeState) bool {
		return n.Name == m.Config.Name ||
			n.Name == node.Name ||
			n.State != StateAlive
	}) // 3 个
	m.NodeLock.RUnlock()

	// 尝试间接Ping。不再直接发送到目标节点，而是询问其他节点目标节点的状态
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
		if ind.Nack = peer.PMax >= 4; ind.Nack {
			expectedNacks++
		}
		// 搜  case IndirectPingMsg:
		if err := m.encodeAndSendMsg(peer.FullAddress(), IndirectPingMsg, &ind); err != nil {
			_ = m.handleIndirectPing
			m.Logger.Printf("[错误] memberlist:间接Ping失败: %s", err)
		}
	}

	// Also make an attempt to contact the node directly over TCP. This
	// helps prevent confused clients who get isolated from UDP traffic
	// but can still speak TCP (which also means they can possibly report
	// misinformation to other Nodes via anti-entropy), avoiding flapping in
	// the cluster.
	// 同时尝试通过TCP直接联系该节点。这有助于防止混乱的客户端被隔离在UDP流量之外，
	// 但仍然可以讲TCP（这也意味着他们有可能通过反熵向其他节点报告错误信息），避免集群中的拍打。
	//
	// This is a little unusual because we will attempt a TCP Ping to any
	// member who understands version 3 of the protocol, regardless of
	// which 协议版本 we are speaking. That's why we've included a
	// Config option to turn this off if desired.
	// 这有点不寻常，因为我们将尝试向任何理解协议版本3的成员进行TCP Ping，而不管我们说的是哪个协议版本。这就是为什么我们包括一个配置选项，如果需要的话，可以关闭这个功能。
	fallbackCh := make(chan bool, 1)

	disableTcpPings := m.Config.DisableTcpPings || (m.Config.DisableTcpPingsForNode != nil && m.Config.DisableTcpPingsForNode(node.Name))
	// 是否禁止使用TCP Ping
	_ = ProtocolVersionMax
	if (!disableTcpPings) && (node.PMax >= 3) {
		go func() {
			defer close(fallbackCh)
			didContact, err := m.SendPingAndWaitForAck(node.FullAddress(), ping, Deadline)
			if err != nil {
				var to string
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					to = fmt.Sprintf("timeout %s: ", probeInterval)
				}
				m.Logger.Printf("[错误] memberlist: 失败 fallback Ping: %s%s", to, err)
			} else {
				fallbackCh <- didContact
			}
		}()
	} else {
		close(fallbackCh)
	}

	// UDP间接PING  可能会收到ack信号
	select {
	case v := <-ackCh: // 只判断第一条返回的消息
		if v.Complete == true { // 超时返回FALSE
			return
		}
	}
	// UDP 没有成功
	for didContact := range fallbackCh { // 阻塞等待结果
		if didContact {
			m.Logger.Printf("[WARN] memberlist: 能够连接到%s，但其他探测失败，网络可能被错误配置了", node.Name)
			return
		}
	}
	// TCP 没有成功

	// 根据这次失败的探测结果，更新我们的自我警觉。如果我们没有同伴会发送nacks，那么我们会对任何失败的探测进行惩罚，
	// 作为一个简单的健康指标。如果我们有对等人进行nack验证，那么我们可以把它作为一个更复杂的自我健康度量，
	// 因为我们假设他们在工作，他们可以帮助我们决定被探测的节点是否真的死亡，或者是我们自己出了问题。
	awarenessDelta = 0
	if expectedNacks > 0 { //期望收到的 未确认消息
		if nackCount := len(nackCh); nackCount < expectedNacks {
			// expectedNacks - nackCount 有几个节点没有发送Nack消息  (可能是网络延迟、或根本就没有发送)
			awarenessDelta += expectedNacks - nackCount
		}
	} else {
		awarenessDelta += 1
	}

	// 没有收到来自目标的ack消息，怀疑是失败的。
	m.Logger.Printf("[INFO] memberlist: 怀疑 %s has failed,没有收到ack消息", node.Name)
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

	a := pkg.Address{Addr: Addr.String(), Name: node}
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
