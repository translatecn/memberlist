package memberlist

import (
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

type NodeStateType int

const (
	StateAlive NodeStateType = iota
	StateSuspect
	StateDead
	StateLeft
)

// Node 体现了一个集群中节点的状态
type Node struct {
	Name  string
	Addr  net.IP
	Port  uint16
	Meta  []byte        // 节点委托的元信息
	State NodeStateType // State of the node.
	PMin  uint8         // Minimum 协议版本 this understands
	PMax  uint8         // Maximum 协议版本 this understands
	PCur  uint8         // Current version node is speaking
	DMin  uint8         // Min 协议版本 for the delegate to understand
	DMax  uint8         // Max 协议版本 for the delegate to understand
	DCur  uint8         // Current version delegate is speaking
}

// Address returns the host:Port form of a node's Address, suitable for use
// with a Transport.
func (n *Node) Address() string {
	return pkg.JoinHostPort(n.Addr.String(), n.Port)
}

// FullAddress returns the node name and host:Port form of a node's Address,
// suitable for use with a Transport.
func (n *Node) FullAddress() Address {
	return Address{
		Addr: pkg.JoinHostPort(n.Addr.String(), n.Port),
		Name: n.Name,
	}
}

// String returns the node name
func (n *Node) String() string {
	return n.Name
}

// NodeState 管理其他节点的状态视图
type NodeState struct {
	Node
	Incarnation uint32        // Last known incarnation number
	State       NodeStateType // 当前的状态
	StateChange time.Time     // Time last state change happened
}

// Address returns the host:Port form of a node's Address, suitable for use
// with a Transport.
func (n *NodeState) Address() string {
	return n.Node.Address()
}

// FullAddress returns the node name and host:Port form of a node's Address,
// suitable for use with a Transport.
func (n *NodeState) FullAddress() Address {
	return n.Node.FullAddress()
}

func (n *NodeState) DeadOrLeft() bool {
	return n.State == StateDead || n.State == StateLeft
}

// AckHandler 注册确认、否认处理函数
type AckHandler struct {
	ackFn  func([]byte, time.Time)
	nackFn func()
	timer  *time.Timer
}

// NoPingResponseError is used to indicate a 'Ping' packet was
// successfully issued but no response was received
type NoPingResponseError struct {
	node string
}

func (f NoPingResponseError) Error() string {
	return fmt.Sprintf("No response from node %s", f.node)
}

// Schedule 开启定时器
func (m *Members) Schedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()
	if len(m.tickers) > 0 {
		return
	}

	stopCh := make(chan struct{})

	// 开始定时探活
	if m.Config.ProbeInterval > 0 {
		t := time.NewTicker(m.Config.ProbeInterval) // Config.go:190 1s
		go m.triggerFunc(m.Config.ProbeInterval, t.C, stopCh, m.probe)
		//m.Probe()
		m.tickers = append(m.tickers, t)
	}

	if m.Config.PushPullInterval > 0 {
		// 开始 push、pull 触发器
		go m.PushPullTrigger(stopCh)
	}

	// gossip 定时器
	if m.Config.GossipInterval > 0 && m.Config.GossipNodes > 0 { // 每隔100ms,将消息随机发送到3个节点
		t := time.NewTicker(m.Config.GossipInterval)
		go m.triggerFunc(m.Config.GossipInterval, t.C, stopCh, m.gossip)
		m.tickers = append(m.tickers, t)
	}
	if len(m.tickers) > 0 {
		m.stopTickCh = stopCh
	}
}

// triggerFunc 没收到一条消息就执行f(),知道收到stop信号
func (m *Members) triggerFunc(stagger time.Duration, C <-chan time.Time, stop <-chan struct{}, f func()) {
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(stagger))
	// 开始时 随机睡眠0~stagger
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}
	for {
		select {
		case <-C:
			f()
		case <-stop:
			return
		}
	}
}

// 周期性的push\pull 直到收到stop信号。不使用triggerFunc，因为触发时间不固定，基于集群大小避免网络堵塞
func (m *Members) PushPullTrigger(stop <-chan struct{}) {
	interval := m.Config.PushPullInterval
	// 开始时 随机睡眠0~randStagger
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(interval))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}
	// 使用动态的ticker
	for {
		tickTime := PushPullScale(interval, m.EstNumNodes())
		select {
		case <-time.After(tickTime):
			m.PushPull()
		case <-stop:
			return
		}
	}
}

// 停止后台循环
func (m *Members) deschedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()

	// 如果我们没有定时器，那么我们就没有被调度。
	if len(m.tickers) == 0 {
		return
	}

	// 关闭stop channel，使所有的ticker监听者停止。
	close(m.stopTickCh)

	// 关闭定时器
	for _, t := range m.tickers {
		t.Stop()
	}
	m.tickers = nil
}

// 用于对所有节点的探活
func (m *Members) Probe() {
	// 探测过的数量
	numCheck := 0
START:
	m.NodeLock.RLock()

	// 当所有的节点都检查完了一遍
	if numCheck >= len(m.Nodes) {
		m.NodeLock.RUnlock()
		return
	}

	if m.probeIndex >= len(m.Nodes) {
		m.NodeLock.RUnlock()
		m.ResetNodes() // 清理无效节点,打乱顺序
		m.probeIndex = 0
		numCheck++
		goto START
	}
	// 是否跳过节点探测
	skip := false
	var node NodeState

	node = *m.Nodes[m.probeIndex]
	// 本机 或 节点在Dead、left的状态都跳过
	if node.Name == m.Config.Name {
		skip = true
	} else if node.DeadOrLeft() {
		skip = true
	}

	m.NodeLock.RUnlock()
	m.probeIndex++
	if skip {
		numCheck++
		goto START
	}
	m.ProbeNode(&node)
}

// OK
func (m *Members) ProbeNodeByAddr(Addr string) {
	m.NodeLock.RLock()
	n := m.NodeMap[Addr]
	m.NodeLock.RUnlock()

	m.ProbeNode(n)
}

// FailedRemote 检查错误并决定它是否表明另一端有故障。
func FailedRemote(err error) bool {
	switch t := err.(type) {
	case *net.OpError:
		if strings.HasPrefix(t.Net, "tcp") {
			switch t.Op {
			case "dial", "read", "write":
				return true
			}
		} else if strings.HasPrefix(t.Net, "udp") {
			switch t.Op {
			case "write":
				return true
			}
		}
	}
	return false
}

// probeNode 单个节点的故障检查。
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
	// also tack on a Suspect message so that it has a chance to refute as
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

// Ping initiates a Ping to the node with the specified name.
func (m *Members) Ping(node string, Addr net.Addr) (time.Duration, error) {
	// Prepare a Ping message and setup an ack handler.
	selfAddr, selfPort := m.getAdvertise()
	ping := Ping{
		SeqNo:      m.NextSeqNo(),
		Node:       node,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}
	ackCh := make(chan AckMessage, m.Config.IndirectChecks+1)
	m.SetProbeChannels(ping.SeqNo, ackCh, nil, m.Config.ProbeInterval)

	a := Address{Addr: Addr.String(), Name: node}

	// Send a Ping to the node.
	if err := m.encodeAndSendMsg(a, PingMsg, &ping); err != nil {
		return 0, err
	}

	// Mark the sent time here, which should be after any pre-processing and
	// system calls to do the actual send. This probably under-reports a bit,
	// but it's the best we can do.
	sent := time.Now()

	// Wait for response or timeout.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			return v.Timestamp.Sub(sent), nil
		}
	case <-time.After(m.Config.ProbeTimeout):
		// Timeout, return an error below.
	}

	m.Logger.Printf("[DEBUG] memberlist: Failed UDP Ping: %v (timeout reached)", node)
	return 0, NoPingResponseError{ping.Node}
}

// ResetNodes 清除Dead节点,并将节点列表刷新
func (m *Members) ResetNodes() {
	m.NodeLock.Lock()
	defer m.NodeLock.Unlock()

	// 移除Dead node ,超过了Dead interval的
	DeadIdx := MoveDeadNodes(m.Nodes, m.Config.GossipToTheDeadTime)
	// 第一个在m.Nodes Dead的节点的索引
	for i := DeadIdx; i < len(m.Nodes); i++ {
		delete(m.NodeMap, m.Nodes[i].Name)
		m.Nodes[i] = nil
	}

	// 修剪节点以排除死节点
	m.Nodes = m.Nodes[0:DeadIdx]

	// 更新集群节点数量
	atomic.StoreUint32(&m.numNodes, uint32(DeadIdx))

	// 打乱节点列表
	ShuffleNodes(m.Nodes)
}

// gossip is invoked every GossipInterval period to broadcast our gossip
// messages to a few random Nodes.
func (m *Members) Gossip() {

	// Get some random live, Suspect, or recently Dead Nodes
	m.NodeLock.RLock()
	kNodes := KRandomNodes(m.Config.GossipNodes, m.Nodes, func(n *NodeState) bool {
		if n.Name == m.Config.Name {
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

	// Compute the bytes available
	bytesAvail := m.Config.UDPBufferSize - CompoundHeaderOverhead - LabelOverhead(m.Config.Label)
	if m.Config.EncryptionEnabled() {
		bytesAvail -= encryptOverhead(m.EncryptionVersion())
	}

	for _, node := range kNodes {
		// Get any pending Broadcasts
		msgs := m.getBroadcasts(CompoundOverhead, bytesAvail)
		if len(msgs) == 0 {
			return
		}

		Addr := node.Address()
		if len(msgs) == 1 {
			// Send single message as is
			if err := m.RawSendMsgPacket(node.FullAddress(), &node, msgs[0]); err != nil {
				m.Logger.Printf("[错误] memberlist: Failed to send gossip to %s: %s", Addr, err)
			}
		} else {
			// Otherwise create and send one or more compound messages
			compounds := MakeCompoundMessages(msgs)
			for _, compound := range compounds {
				if err := m.RawSendMsgPacket(node.FullAddress(), &node, compound.Bytes()); err != nil {
					m.Logger.Printf("[错误] memberlist: Failed to send gossip to %s: %s", Addr, err)
				}
			}
		}
	}
}

// PushPull is invoked periodically to randomly perform a complete state
// exchange. Used to ensure a high level of convergence, but is also
// reasonably expensive as the entire state of this node is exchanged
// with the other node.
func (m *Members) PushPull() {
	// Get a random live node
	m.NodeLock.RLock()
	nodes := KRandomNodes(1, m.Nodes, func(n *NodeState) bool {
		return n.Name == m.Config.Name ||
			n.State != StateAlive
	})
	m.NodeLock.RUnlock()

	// If no Nodes, bail
	if len(nodes) == 0 {
		return
	}
	node := nodes[0]

	// Attempt a push pull
	if err := m.PushPullNode(node.FullAddress(), false); err != nil {
		m.Logger.Printf("[错误] memberlist: Push/Pull with %s failed: %s", node.Name, err)
	}
}

// PushPullNode 与一个特定的节点进行完整的状态交换。
func (m *Members) PushPullNode(a Address, join bool) error {

	remote, userState, err := m.sendAndReceiveState(a, join)
	if err != nil {
		return err
	}

	if err := m.mergeRemoteState(join, remote, userState); err != nil {
		return err
	}
	return nil
}

// 验证远端传来的NodeState 的协议版本与委托版本
func (m *Members) VerifyProtocol(remote []PushNodeState) error {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()

	var maxpmin, minpmax uint8
	var maxdmin, mindmax uint8
	minpmax = math.MaxUint8
	mindmax = math.MaxUint8

	for _, rn := range remote {
		if rn.State != StateAlive {
			continue
		}

		if len(rn.Vsn) == 0 {
			continue
		}

		if rn.Vsn[0] > maxpmin {
			maxpmin = rn.Vsn[0]
		}

		if rn.Vsn[1] < minpmax {
			minpmax = rn.Vsn[1]
		}

		if rn.Vsn[3] > maxdmin {
			maxdmin = rn.Vsn[3]
		}

		if rn.Vsn[4] < mindmax {
			mindmax = rn.Vsn[4]
		}
	}

	for _, n := range m.Nodes {
		if n.State != StateAlive {
			continue
		}

		if n.PMin > maxpmin {
			maxpmin = n.PMin
		}

		if n.PMax < minpmax {
			minpmax = n.PMax
		}

		if n.DMin > maxdmin {
			maxdmin = n.DMin
		}

		if n.DMax < mindmax {
			mindmax = n.DMax
		}
	}

	for _, n := range remote {
		var nPCur, nDCur uint8
		if len(n.Vsn) > 0 {
			nPCur = n.Vsn[2]
			nDCur = n.Vsn[5]
		}

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf("Node '%s' 协议版本 (%d) is incompatible: [%d, %d]", n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf("Node '%s' delegate 协议版本 (%d) is incompatible: [%d, %d]", n.Name, nDCur, maxdmin, mindmax)
		}
	}

	for _, n := range m.Nodes {
		nPCur := n.PCur
		nDCur := n.DCur

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf(
				"Node '%s' 协议版本 (%d) is incompatible: [%d, %d]",
				n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf("Node '%s' delegate 协议版本 (%d) is incompatible: [%d, %d]", n.Name, nDCur, maxdmin, mindmax)
		}
	}

	return nil
}

// NextSeqNo 以线程安全的方式返回一个可用的序列号
func (m *Members) NextSeqNo() uint32 {
	return atomic.AddUint32(&m.SequenceNum, 1)
}

// nextIncarnation 以线程安全的方式返回下一个incarnation的编号。
func (m *Members) nextIncarnation() uint32 {
	return atomic.AddUint32(&m.incarnation, 1)
}

// skipIncarnation incarnation number添加偏移量
func (m *Members) skipIncarnation(offset uint32) uint32 {
	return atomic.AddUint32(&m.incarnation, offset)
}

// EstNumNodes 用于获得当前估计的节点数
func (m *Members) EstNumNodes() int {
	return int(atomic.LoadUint32(&m.numNodes))
}

type AckMessage struct {
	Complete  bool
	Payload   []byte
	Timestamp time.Time
}

// SetProbeChannels is used to attach the ackCh to receive a message when an ack
// with a given sequence number is received. The `complete` field of the message
// will be false on timeout. Any nack messages will cause an empty struct to be
// passed to the nackCh, which can be nil if not needed.
func (m *Members) SetProbeChannels(seqNo uint32, ackCh chan AckMessage, nackCh chan struct{}, timeout time.Duration) {
	// Create handler functions for acks and nacks
	ackFn := func(payload []byte, timestamp time.Time) {
		select {
		case ackCh <- AckMessage{true, payload, timestamp}:
		default:
		}
	}
	nackFn := func() {
		select {
		case nackCh <- struct{}{}:
		default:
		}
	}

	// Add the handlers
	ah := &AckHandler{ackFn, nackFn, nil}
	m.AckLock.Lock()
	m.AckHandlers[seqNo] = ah
	m.AckLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		m.AckLock.Lock()
		delete(m.AckHandlers, seqNo)
		m.AckLock.Unlock()
		select {
		case ackCh <- AckMessage{false, nil, time.Now()}:
		default:
		}
	})
}

// SetAckHandler is used to attach a handler to be invoked when an ack with a
// given sequence number is received. If a timeout is reached, the handler is
// deleted. This is used for indirect pings so does not configure a function
// for nacks.
func (m *Members) SetAckHandler(seqNo uint32, ackFn func([]byte, time.Time), timeout time.Duration) {
	// Add the handler
	ah := &AckHandler{ackFn, nil, nil}
	m.AckLock.Lock()
	m.AckHandlers[seqNo] = ah
	m.AckLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		m.AckLock.Lock()
		delete(m.AckHandlers, seqNo)
		m.AckLock.Unlock()
	})
}

// Invokes an ack handler if any is associated, and reaps the handler immediately
func (m *Members) InvokeAckHandler(ack AckResp, timestamp time.Time) {
	m.AckLock.Lock()
	ah, ok := m.AckHandlers[ack.SeqNo]
	delete(m.AckHandlers, ack.SeqNo)
	m.AckLock.Unlock()
	if !ok {
		return
	}
	ah.timer.Stop()
	ah.ackFn(ack.Payload, timestamp)
}

// Invokes nack handler if any is associated.
func (m *Members) InvokeNAckHandler(nack NAckResp) {
	m.AckLock.Lock()
	ah, ok := m.AckHandlers[nack.SeqNo]
	m.AckLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	ah.nackFn()
}

// 当收到传来的关于本节点被怀疑或死亡的信息时，会发送一个Alive gossip message。
// 它将确保incarnation超过给定的 accusedInc 值，或者你可以提供 0 来获取下一个incarnation。
// 这将改变传入的节点状态，所以必须在持有NodeLock情况下调用这个。
func (m *Members) refute(me *NodeState, accusedInc uint32) {
	inc := m.nextIncarnation()
	if accusedInc >= inc {
		inc = m.skipIncarnation(accusedInc - inc + 1)
	}
	me.Incarnation = inc

	// 减少health，因为我们被要求反驳一个问题。
	m.Awareness.ApplyDelta(1)

	a := Alive{
		Incarnation: inc,
		Node:        me.Name,
		Addr:        me.Addr,
		Port:        me.Port,
		Meta:        me.Meta,
		Vsn: []uint8{
			me.PMin, me.PMax, me.PCur,
			me.DMin, me.DMax, me.DCur,
		},
	}
	m.EncodeBroadcast(me.Addr.String(), AliveMsg, a)
}
