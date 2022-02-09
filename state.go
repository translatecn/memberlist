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

// Address returns the host:port form of a node's address, suitable for use
// with a transport.
func (n *Node) Address() string {
	return pkg.JoinHostPort(n.Addr.String(), n.Port)
}

// FullAddress returns the node name and host:port form of a node's address,
// suitable for use with a transport.
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
type nodeState struct {
	Node
	Incarnation uint32        // Last known incarnation number
	State       NodeStateType // 当前的状态
	StateChange time.Time     // Time last state change happened
}

// Address returns the host:port form of a node's address, suitable for use
// with a transport.
func (n *nodeState) Address() string {
	return n.Node.Address()
}

// FullAddress returns the node name and host:port form of a node's address,
// suitable for use with a transport.
func (n *nodeState) FullAddress() Address {
	return n.Node.FullAddress()
}

func (n *nodeState) DeadOrLeft() bool {
	return n.State == StateDead || n.State == StateLeft
}

// ackHandler 注册确认、否认处理函数
type ackHandler struct {
	ackFn  func([]byte, time.Time)
	nackFn func()
	timer  *time.Timer
}

// NoPingResponseError is used to indicate a 'ping' packet was
// successfully issued but no response was received
type NoPingResponseError struct {
	node string
}

func (f NoPingResponseError) Error() string {
	return fmt.Sprintf("No response from node %s", f.node)
}

// 开启定时器
func (m *Members) schedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()
	if len(m.tickers) > 0 {
		return
	}

	stopCh := make(chan struct{})

	// 开始定时探活
	if m.config.ProbeInterval > 0 {
		t := time.NewTicker(m.config.ProbeInterval) // config.go:190 1s
		go m.triggerFunc(m.config.ProbeInterval, t.C, stopCh, m.probe)
		//m.probe()
		m.tickers = append(m.tickers, t)
	}

	if m.config.PushPullInterval > 0 {
		// 开始 push、pull 触发器
		go m.pushPullTrigger(stopCh)
	}

	// gossip 定时器
	if m.config.GossipInterval > 0 && m.config.GossipNodes > 0 { // 每隔100ms,将消息随机发送到3个节点
		t := time.NewTicker(m.config.GossipInterval)
		go m.triggerFunc(m.config.GossipInterval, t.C, stopCh, m.gossip)
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
func (m *Members) pushPullTrigger(stop <-chan struct{}) {
	interval := m.config.PushPullInterval
	// 开始时 随机睡眠0~randStagger
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(interval))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}
	// 使用动态的ticker
	for {
		tickTime := pushPullScale(interval, m.estNumNodes())
		select {
		case <-time.After(tickTime):
			m.pushPull()
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
func (m *Members) probe() {
	// 探测过的数量
	numCheck := 0
START:
	m.nodeLock.RLock()

	// 当所有的节点都检查完了一遍
	if numCheck >= len(m.nodes) {
		m.nodeLock.RUnlock()
		return
	}

	if m.probeIndex >= len(m.nodes) {
		m.nodeLock.RUnlock()
		m.resetNodes() // 清理无效节点,打乱顺序
		m.probeIndex = 0
		numCheck++
		goto START
	}
	// 是否跳过节点探测
	skip := false
	var node nodeState

	node = *m.nodes[m.probeIndex]
	// 本机 或 节点在dead、left的状态都跳过
	if node.Name == m.config.Name {
		skip = true
	} else if node.DeadOrLeft() {
		skip = true
	}

	m.nodeLock.RUnlock()
	m.probeIndex++
	if skip {
		numCheck++
		goto START
	}
	m.probeNode(&node)
}

// OK
func (m *Members) probeNodeByAddr(addr string) {
	m.nodeLock.RLock()
	n := m.nodeMap[addr]
	m.nodeLock.RUnlock()

	m.probeNode(n)
}

// failedRemote 检查错误并决定它是否表明另一端有故障。
func failedRemote(err error) bool {
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
func (m *Members) probeNode(node *nodeState) {

	// 我们使用我们的health awareness来扩展整个探测间隔，
	// 所以如果我们检测到问题，我们会放慢速度。
	// 调用我们的探测器可以处理我们运行超过基本间隔的情况，并会跳过错过的刻度。
	probeInterval := m.Awareness.ScaleTimeout(m.config.ProbeInterval) // 根据健康度、设置探活时间

	selfAddr, selfPort := m.getAdvertise()
	ping := ping{
		SeqNo:      m.nextSeqNo(),
		Node:       node.Name,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.config.Name,
	}
	ackCh := make(chan ackMessage, m.config.IndirectChecks+1)
	nackCh := make(chan struct{}, m.config.IndirectChecks+1)
	m.setProbeChannels(ping.SeqNo, ackCh, nackCh, probeInterval)

	// Mark the sent time here, which should be after any pre-processing but
	// before system calls to do the actual send. This probably over-reports
	// a bit, but it's the best we can do. We had originally put this right
	// after the I/O, but that would sometimes give negative RTT measurements
	// which was not desirable.
	sent := time.Now()

	// Send a ping to the node. If this node looks like it's suspect or dead,
	// also tack on a suspect message so that it has a chance to refute as
	// soon as possible.
	deadline := sent.Add(probeInterval)
	addr := node.Address()

	// Arrange for our self-awareness to get updated.
	var awarenessDelta int
	defer func() {
		m.Awareness.ApplyDelta(awarenessDelta)
	}()
	if node.State == StateAlive {
		if err := m.encodeAndSendMsg(node.FullAddress(), pingMsg, &ping); err != nil {
			m.logger.Printf("[错误] memberlist: Failed to send ping: %s", err)
			if failedRemote(err) {
				goto HANDLE_REMOTE_FAILURE
			} else {
				return
			}
		}
	} else {
		var msgs [][]byte
		if buf, err := encode(pingMsg, &ping); err != nil {
			m.logger.Printf("[错误] memberlist: Failed to encode ping message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}
		s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.config.Name}
		if buf, err := encode(suspectMsg, &s); err != nil {
			m.logger.Printf("[错误] memberlist: Failed to encode suspect message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}

		compound := makeCompoundMessage(msgs)
		if err := m.rawSendMsgPacket(node.FullAddress(), &node.Node, compound.Bytes()); err != nil {
			m.logger.Printf("[错误] memberlist: Failed to send compound ping and suspect message to %s: %s", addr, err)
			if failedRemote(err) {
				goto HANDLE_REMOTE_FAILURE
			} else {
				return
			}
		}
	}

	// Arrange for our self-awareness to get updated. At this point we've
	// sent the ping, so any return statement means the probe succeeded
	// which will improve our health until we get to the failure scenarios
	// at the end of this function, which will alter this delta variable
	// accordingly.
	awarenessDelta = -1

	// Wait for response or round-trip-time.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			if m.config.Ping != nil {
				rtt := v.Timestamp.Sub(sent)
				m.config.Ping.NotifyPingComplete(&node.Node, rtt, v.Payload)
			}
			return
		}

		// As an edge case, if we get a timeout, we need to re-enqueue it
		// here to break out of the select below.
		if v.Complete == false {
			ackCh <- v
		}
	case <-time.After(m.config.ProbeTimeout):
		// Note that we don't scale this timeout based on awareness and
		// the health score. That's because we don't really expect waiting
		// longer to help get UDP through. Since health does extend the
		// probe interval it will give the TCP fallback more time, which
		// is more active in dealing with lost packets, and it gives more
		// time to wait for indirect acks/nacks.
		m.logger.Printf("[DEBUG] memberlist: Failed ping: %s (timeout reached)", node.Name)
	}

HANDLE_REMOTE_FAILURE:
	// Get some random live nodes.
	m.nodeLock.RLock()
	kNodes := kRandomNodes(m.config.IndirectChecks, m.nodes, func(n *nodeState) bool {
		return n.Name == m.config.Name ||
			n.Name == node.Name ||
			n.State != StateAlive
	})
	m.nodeLock.RUnlock()

	// Attempt an indirect ping.
	expectedNacks := 0
	selfAddr, selfPort = m.getAdvertise()
	ind := indirectPingReq{
		SeqNo:      ping.SeqNo,
		Target:     node.Addr,
		Port:       node.Port,
		Node:       node.Name,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.config.Name,
	}
	for _, peer := range kNodes {
		// We only expect nack to be sent from peers who understand
		// version 4 of the protocol.
		if ind.Nack = peer.PMax >= 4; ind.Nack {
			expectedNacks++
		}

		if err := m.encodeAndSendMsg(peer.FullAddress(), indirectPingMsg, &ind); err != nil {
			m.logger.Printf("[错误] memberlist: Failed to send indirect ping: %s", err)
		}
	}

	// Also make an attempt to contact the node directly over TCP. This
	// helps prevent confused clients who get isolated from UDP traffic
	// but can still speak TCP (which also means they can possibly report
	// misinformation to other nodes via anti-entropy), avoiding flapping in
	// the cluster.
	//
	// This is a little unusual because we will attempt a TCP ping to any
	// member who understands version 3 of the protocol, regardless of
	// which 协议版本 we are speaking. That's why we've included a
	// config option to turn this off if desired.
	fallbackCh := make(chan bool, 1)

	disableTcpPings := m.config.DisableTcpPings ||
		(m.config.DisableTcpPingsForNode != nil && m.config.DisableTcpPingsForNode(node.Name))
	if (!disableTcpPings) && (node.PMax >= 3) {
		go func() {
			defer close(fallbackCh)
			didContact, err := m.sendPingAndWaitForAck(node.FullAddress(), ping, deadline)
			if err != nil {
				var to string
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					to = fmt.Sprintf("timeout %s: ", probeInterval)
				}
				m.logger.Printf("[错误] memberlist: Failed fallback ping: %s%s", to, err)
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
			m.logger.Printf("[WARN] memberlist: Was able to connect to %s but other probes failed, network may be misconfigured", node.Name)
			return
		}
	}

	// Update our self-awareness based on the results of this failed probe.
	// If we don't have peers who will send nacks then we penalize for any
	// failed probe as a simple health metric. If we do have peers to nack
	// verify, then we can use that as a more sophisticated measure of self-
	// health because we assume them to be working, and they can help us
	// decide if the probed node was really dead or if it was something wrong
	// with ourselves.
	awarenessDelta = 0
	if expectedNacks > 0 {
		if nackCount := len(nackCh); nackCount < expectedNacks {
			awarenessDelta += (expectedNacks - nackCount)
		}
	} else {
		awarenessDelta += 1
	}

	// No acks received from target, suspect it as failed.
	m.logger.Printf("[INFO] memberlist: Suspect %s has failed, no acks received", node.Name)
	s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.config.Name}
	m.suspectNode(&s)
}

// Ping initiates a ping to the node with the specified name.
func (m *Members) Ping(node string, addr net.Addr) (time.Duration, error) {
	// Prepare a ping message and setup an ack handler.
	selfAddr, selfPort := m.getAdvertise()
	ping := ping{
		SeqNo:      m.nextSeqNo(),
		Node:       node,
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.config.Name,
	}
	ackCh := make(chan ackMessage, m.config.IndirectChecks+1)
	m.setProbeChannels(ping.SeqNo, ackCh, nil, m.config.ProbeInterval)

	a := Address{Addr: addr.String(), Name: node}

	// Send a ping to the node.
	if err := m.encodeAndSendMsg(a, pingMsg, &ping); err != nil {
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
	case <-time.After(m.config.ProbeTimeout):
		// Timeout, return an error below.
	}

	m.logger.Printf("[DEBUG] memberlist: Failed UDP ping: %v (timeout reached)", node)
	return 0, NoPingResponseError{ping.Node}
}

// resetNodes 清除dead节点,并将节点列表刷新
func (m *Members) resetNodes() {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	// 移除dead node ,超过了dead interval的
	deadIdx := moveDeadNodes(m.nodes, m.config.GossipToTheDeadTime)
	// 第一个在m.nodes dead的节点的索引
	for i := deadIdx; i < len(m.nodes); i++ {
		delete(m.nodeMap, m.nodes[i].Name)
		m.nodes[i] = nil
	}

	// 修剪节点以排除死节点
	m.nodes = m.nodes[0:deadIdx]

	// 更新集群节点数量
	atomic.StoreUint32(&m.numNodes, uint32(deadIdx))

	// 打乱节点列表
	shuffleNodes(m.nodes)
}

// gossip is invoked every GossipInterval period to broadcast our gossip
// messages to a few random nodes.
func (m *Members) gossip() {

	// Get some random live, suspect, or recently dead nodes
	m.nodeLock.RLock()
	kNodes := kRandomNodes(m.config.GossipNodes, m.nodes, func(n *nodeState) bool {
		if n.Name == m.config.Name {
			return true
		}

		switch n.State {
		case StateAlive, StateSuspect:
			return false

		case StateDead:
			return time.Since(n.StateChange) > m.config.GossipToTheDeadTime

		default:
			return true
		}
	})
	m.nodeLock.RUnlock()

	// Compute the bytes available
	bytesAvail := m.config.UDPBufferSize - compoundHeaderOverhead - labelOverhead(m.config.Label)
	if m.config.EncryptionEnabled() {
		bytesAvail -= encryptOverhead(m.encryptionVersion())
	}

	for _, node := range kNodes {
		// Get any pending broadcasts
		msgs := m.getBroadcasts(compoundOverhead, bytesAvail)
		if len(msgs) == 0 {
			return
		}

		addr := node.Address()
		if len(msgs) == 1 {
			// Send single message as is
			if err := m.rawSendMsgPacket(node.FullAddress(), &node, msgs[0]); err != nil {
				m.logger.Printf("[错误] memberlist: Failed to send gossip to %s: %s", addr, err)
			}
		} else {
			// Otherwise create and send one or more compound messages
			compounds := makeCompoundMessages(msgs)
			for _, compound := range compounds {
				if err := m.rawSendMsgPacket(node.FullAddress(), &node, compound.Bytes()); err != nil {
					m.logger.Printf("[错误] memberlist: Failed to send gossip to %s: %s", addr, err)
				}
			}
		}
	}
}

// pushPull is invoked periodically to randomly perform a complete state
// exchange. Used to ensure a high level of convergence, but is also
// reasonably expensive as the entire state of this node is exchanged
// with the other node.
func (m *Members) pushPull() {
	// Get a random live node
	m.nodeLock.RLock()
	nodes := kRandomNodes(1, m.nodes, func(n *nodeState) bool {
		return n.Name == m.config.Name ||
			n.State != StateAlive
	})
	m.nodeLock.RUnlock()

	// If no nodes, bail
	if len(nodes) == 0 {
		return
	}
	node := nodes[0]

	// Attempt a push pull
	if err := m.pushPullNode(node.FullAddress(), false); err != nil {
		m.logger.Printf("[错误] memberlist: Push/Pull with %s failed: %s", node.Name, err)
	}
}

// pushPullNode 与一个特定的节点进行完整的状态交换。
func (m *Members) pushPullNode(a Address, join bool) error {

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
func (m *Members) verifyProtocol(remote []pushNodeState) error {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

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

	for _, n := range m.nodes {
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

	for _, n := range m.nodes {
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

// nextSeqNo 以线程安全的方式返回一个可用的序列号
func (m *Members) nextSeqNo() uint32 {
	return atomic.AddUint32(&m.sequenceNum, 1)
}

// nextIncarnation 以线程安全的方式返回下一个incarnation的编号。
func (m *Members) nextIncarnation() uint32 {
	return atomic.AddUint32(&m.incarnation, 1)
}

// skipIncarnation incarnation number添加偏移量
func (m *Members) skipIncarnation(offset uint32) uint32 {
	return atomic.AddUint32(&m.incarnation, offset)
}

// estNumNodes 用于获得当前估计的节点数
func (m *Members) estNumNodes() int {
	return int(atomic.LoadUint32(&m.numNodes))
}

type ackMessage struct {
	Complete  bool
	Payload   []byte
	Timestamp time.Time
}

// setProbeChannels is used to attach the ackCh to receive a message when an ack
// with a given sequence number is received. The `complete` field of the message
// will be false on timeout. Any nack messages will cause an empty struct to be
// passed to the nackCh, which can be nil if not needed.
func (m *Members) setProbeChannels(seqNo uint32, ackCh chan ackMessage, nackCh chan struct{}, timeout time.Duration) {
	// Create handler functions for acks and nacks
	ackFn := func(payload []byte, timestamp time.Time) {
		select {
		case ackCh <- ackMessage{true, payload, timestamp}:
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
	ah := &ackHandler{ackFn, nackFn, nil}
	m.ackLock.Lock()
	m.ackHandlers[seqNo] = ah
	m.ackLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		m.ackLock.Lock()
		delete(m.ackHandlers, seqNo)
		m.ackLock.Unlock()
		select {
		case ackCh <- ackMessage{false, nil, time.Now()}:
		default:
		}
	})
}

// setAckHandler is used to attach a handler to be invoked when an ack with a
// given sequence number is received. If a timeout is reached, the handler is
// deleted. This is used for indirect pings so does not configure a function
// for nacks.
func (m *Members) setAckHandler(seqNo uint32, ackFn func([]byte, time.Time), timeout time.Duration) {
	// Add the handler
	ah := &ackHandler{ackFn, nil, nil}
	m.ackLock.Lock()
	m.ackHandlers[seqNo] = ah
	m.ackLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		m.ackLock.Lock()
		delete(m.ackHandlers, seqNo)
		m.ackLock.Unlock()
	})
}

// Invokes an ack handler if any is associated, and reaps the handler immediately
func (m *Members) invokeAckHandler(ack ackResp, timestamp time.Time) {
	m.ackLock.Lock()
	ah, ok := m.ackHandlers[ack.SeqNo]
	delete(m.ackHandlers, ack.SeqNo)
	m.ackLock.Unlock()
	if !ok {
		return
	}
	ah.timer.Stop()
	ah.ackFn(ack.Payload, timestamp)
}

// Invokes nack handler if any is associated.
func (m *Members) invokeNackHandler(nack nackResp) {
	m.ackLock.Lock()
	ah, ok := m.ackHandlers[nack.SeqNo]
	m.ackLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	ah.nackFn()
}

// 当收到传来的关于本节点被怀疑或死亡的信息时，会发送一个alive gossip message。
// 它将确保incarnation超过给定的 accusedInc 值，或者你可以提供 0 来获取下一个incarnation。
// 这将改变传入的节点状态，所以必须在持有nodeLock情况下调用这个。
func (m *Members) refute(me *nodeState, accusedInc uint32) {
	inc := m.nextIncarnation()
	if accusedInc >= inc {
		inc = m.skipIncarnation(accusedInc - inc + 1)
	}
	me.Incarnation = inc

	// 减少health，因为我们被要求反驳一个问题。
	m.Awareness.ApplyDelta(1)

	a := alive{
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
	m.encodeBroadcast(me.Addr.String(), aliveMsg, a)
}
