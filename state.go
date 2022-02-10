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

// Address 返回host:Port
func (n *Node) Address() string {
	return pkg.JoinHostPort(n.Addr.String(), n.Port)
}

// FullAddress 返回Address
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
		go m.triggerFunc(m.Config.ProbeInterval, t.C, stopCh, m.Probe)
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
		go m.triggerFunc(m.Config.GossipInterval, t.C, stopCh, m.Gossip)
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

// PushPullTrigger 周期性的push\pull 直到收到stop信号。不使用triggerFunc，因为触发时间不固定，基于集群大小避免网络堵塞
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

// Probe 用于对所有节点的探活
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

// ProbeNodeByAddr OK
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

// NextIncarnation 以线程安全的方式返回下一个incarnation的编号。
func (m *Members) NextIncarnation() uint32 {
	return atomic.AddUint32(&m.incarnation, 1)
}
func (m *Members) CurIncarnation() uint32 {
	return m.incarnation
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

// SetProbeChannels 当消息确认时 发到ackCh。如果超时 complete字段会是false
// 不需要确认，发送空struct或nil
func (m *Members) SetProbeChannels(seqNo uint32, ackCh chan AckMessage, nackCh chan struct{}, timeout time.Duration) {
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

	ah := &AckHandler{ackFn, nackFn, nil}
	m.AckLock.Lock()
	m.AckHandlers[seqNo] = ah
	m.AckLock.Unlock()

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



// Refute 当收到传来的关于本节点被怀疑或死亡的信息时，会发送一个Alive gossip message。
// 它将确保incarnation超过给定的 accusedInc 值，或者你可以提供 0 来获取下一个incarnation。
// 这将改变传入的节点状态，所以必须在持有NodeLock情况下调用这个。
func (m *Members) Refute(me *NodeState, accusedInc uint32) {
	inc := m.NextIncarnation()
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

// VerifyProtocol 验证远端传来的NodeState 的协议版本与委托版本
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
