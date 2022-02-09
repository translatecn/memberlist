// Package memberlist /*
package memberlist

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/miekg/dns"
)

//memberlist是一个管理集群的库
//使用基于gossip协议的成员和成员故障检测。
//
//这种库的用例影响深远:所有的分布式系统
//要求成员资格，而成员名单是一个可重用的解决方案来管理
//集群成员和节点故障检测。
//
//成员列表最终是一致的，但平均上很快收敛。
//它的收敛速度可以通过各种各样的旋钮来调整
//在协议。检测到节点故障，部分网络分区
//可以通过尝试与潜在的死节点进行通信
//多个路线。

var errNodeNamesAreRequired = errors.New("memberlist: 配置需要节点名，但没有提供节点名")

type Members struct {
	sequenceNum uint32 // 本地序列号
	incarnation uint32 // Local incarnation number
	numNodes    uint32 // 已知节点数(估计)
	pushPullReq uint32 // push/pull 请求数

	advertiseLock sync.RWMutex
	advertiseAddr net.IP
	advertisePort uint16

	config         *Config
	shutdown       int32 // 停止标志
	shutdownCh     chan struct{}
	leave          int32
	leaveBroadcast chan struct{} // 离开广播

	shutdownLock sync.Mutex
	leaveLock    sync.Mutex

	transport            NodeAwareTransport
	handoffCh            chan struct{} //TODO 消息队列
	highPriorityMsgQueue *list.List
	lowPriorityMsgQueue  *list.List
	msgQueueLock         sync.Mutex

	nodeLock   sync.RWMutex
	nodes      []*nodeState          // Known nodes
	nodeMap    map[string]*nodeState // Maps Node.Name -> NodeState
	nodeTimers map[string]*suspicion // Maps Node.Name -> suspicion timer
	awareness  *awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTickCh chan struct{}
	probeIndex int

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	broadcasts *TransmitLimitedQueue

	logger *log.Logger
}

// BuildVsnArray 创建Vsn数组
func (c *Config) BuildVsnArray() []uint8 {
	return []uint8{
		ProtocolVersionMin, ProtocolVersionMax, c.ProtocolVersion,
		c.DelegateProtocolMin, c.DelegateProtocolMax, c.DelegateProtocolVersion,
	}
}

// newMembers 创建网络监听器,只能在主线程被调度
func newMembers(conf *Config) (*Members, error) {
	if conf.ProtocolVersion < ProtocolVersionMin {
		return nil, fmt.Errorf("协议版本 '%d' 太小. 必须在这个范围: [%d, %d]", conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	} else if conf.ProtocolVersion > ProtocolVersionMax {
		return nil, fmt.Errorf("协议版本 '%d' 太高. 必须在这个范围: [%d, %d]", conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}

	if len(conf.SecretKey) > 0 {
		if conf.Keyring == nil {
			keyring, err := NewKeyring(nil, conf.SecretKey)
			if err != nil {
				return nil, err
			}
			conf.Keyring = keyring
		} else {
			if err := conf.Keyring.AddKey(conf.SecretKey); err != nil {
				return nil, err
			}
			if err := conf.Keyring.UseKey(conf.SecretKey); err != nil {
				return nil, err
			}
		}
	}

	if conf.LogOutput != nil && conf.Logger != nil {
		return nil, fmt.Errorf("不能同时指定LogOutput和Logger。请选择一个单一的日志配置设置。")
	}

	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	// 如果配置中没有给出自定义的网络传输，则默认设置网络传输。
	transport := conf.Transport // 默认为nil
	if transport == nil {
		nc := &NetTransportConfig{
			BindAddrs: []string{conf.BindAddr}, // 0.0.0.0
			BindPort:  conf.BindPort,
			Logger:    logger,
		}

		// 关于重试的详细信息，请参阅下面的注释。
		makeNetRetry := func(limit int) (*NetTransport, error) {
			var err error
			for try := 0; try < limit; try++ {
				var nt *NetTransport
				if nt, err = NewNetTransport(nc); err == nil {
					return nt, nil
				}
				if strings.Contains(err.Error(), "已使用地址") {
					logger.Printf("[DEBUG] Got bind error: %v", err)
					continue
				}
			}

			return nil, fmt.Errorf("获取地址失败: %v", err)
		}

		// 动态绑定端口的操作本质上是荒谬的，因为即使我们使用内核为我们找到一个端口，我们也试图用同一个端口号来绑定多个协议（以及潜在的多个地址）。
		// 我们在这里设置了一些重试，因为这在繁忙的单元测试中经常会出现瞬时错误。
		limit := 1
		if conf.BindPort == 0 {
			limit = 10
		}

		nt, err := makeNetRetry(limit)
		if err != nil {
			return nil, fmt.Errorf("无法设置网络传输: %v", err)
		}
		if conf.BindPort == 0 {
			// 如果是0,那么NewNetTransport里的端口就是上边随机生成的  GetAutoBindPort多次调用获取的端口是一样的
			port := nt.GetAutoBindPort()
			conf.BindPort = port
			conf.AdvertisePort = port
			logger.Printf("[DEBUG] 使用动态绑定端口 %d", port)
		}
		transport = nt
	}

	nodeAwareTransport, ok := transport.(NodeAwareTransport)
	if !ok {
		logger.Printf("[DEBUG] memberlist: 配置的transport不是一个NodeAwareTransport，一些功能可能无法正常工作。")
		nodeAwareTransport = &shimNodeAwareTransport{transport}
	}

	if len(conf.Label) > LabelMaxSize {
		return nil, fmt.Errorf("不能使用 %q 作为标签: 太长了", conf.Label)
	}

	if conf.Label != "" {
		nodeAwareTransport = &labelWrappedTransport{
			label:              conf.Label,
			NodeAwareTransport: nodeAwareTransport,
		}
	}

	m := &Members{
		config:               conf,
		shutdownCh:           make(chan struct{}),
		leaveBroadcast:       make(chan struct{}, 1), //
		transport:            nodeAwareTransport,
		handoffCh:            make(chan struct{}, 1),
		highPriorityMsgQueue: list.New(), // 高优先级消息队列
		lowPriorityMsgQueue:  list.New(), // 低优先级消息队列
		nodeMap:              make(map[string]*nodeState),
		nodeTimers:           make(map[string]*suspicion),
		awareness:            newAwareness(conf.AwarenessMaxMultiplier), // 感知对象
		ackHandlers:          make(map[uint32]*ackHandler),
		broadcasts:           &TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		logger:               logger,
	}
	m.broadcasts.NumNodes = func() int {
		return m.estNumNodes()
	}

	// 设置广播地址
	if _, _, err := m.refreshAdvertise(); err != nil {
		return nil, err
	}

	go m.streamListen()  // pull 模式
	go m.packetListen()  // 直接消息传递
	go m.packetHandler() //TODO 用于处理消息
	return m, nil
}

// Create 不会链接其他节点、但会开启listeners,允许其他节点加入；之后Config不应该被改变
func Create(conf *Config) (*Members, error) {
	m, err := newMembers(conf) // ok
	if err != nil {
		return nil, err
	}
	if err := m.setAlive(); err != nil {
		m.Shutdown()
		return nil, err
	}
	m.schedule() // 开启各种定时器
	return m, nil
}

// Join is used to take an existing Members and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Members only contains our own state, so doing this will cause
// remote nodes to become aware of the existence of this node, effectively
// joining the cluster.
//
// This returns the number of hosts successfully contacted and an error if
// none could be reached. If an error is returned, the node did not successfully
// join the cluster.
func (m *Members) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		addrs, err := m.resolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("Failed to resolve %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			m.logger.Printf("[WARN] memberlist: %v", err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			a := Address{Addr: hp, Name: addr.nodeName}
			if err := m.pushPullNode(a, true); err != nil {
				err = fmt.Errorf("Failed to join %s: %v", a.Addr, err)
				errs = multierror.Append(errs, err)
				m.logger.Printf("[DEBUG] memberlist: %v", err)
				continue
			}
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

// ipPort holds information about a node we want to try to join.
type ipPort struct {
	ip       net.IP
	port     uint16
	nodeName string // optional
}

// tcpLookupIP is a helper to initiate a TCP-based DNS lookup for the given host.
// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
// the response has the truncate bit set, which isn't common on DNS servers like
// Consul's. By doing the TCP lookup directly, we get the best chance for the
// largest list of hosts to join. Since joins are relatively rare events, it's ok
// to do this rather expensive operation.
func (m *Members) tcpLookupIP(host string, defaultPort uint16, nodeName string) ([]ipPort, error) {
	// Don't attempt any TCP lookups against non-fully qualified domain
	// names, since those will likely come from the resolv.conf file.
	if !strings.Contains(host, ".") {
		return nil, nil
	}

	// Make sure the domain name is terminated with a dot (we know there's
	// at least one character at this point).
	dn := host
	if dn[len(dn)-1] != '.' {
		dn = dn + "."
	}

	// See if we can find a server to try.
	cc, err := dns.ClientConfigFromFile(m.config.DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {
		// We support host:port in the DNS config, but need to add the
		// default port if one is not supplied.
		server := cc.Servers[0]
		if !hasPort(server) {
			server = net.JoinHostPort(server, cc.Port)
		}

		// Do the lookup.
		c := new(dns.Client)
		c.Net = "tcp"
		msg := new(dns.Msg)
		msg.SetQuestion(dn, dns.TypeANY)
		in, _, err := c.Exchange(msg, server)
		if err != nil {
			return nil, err
		}

		// Handle any IPs we get back that we can attempt to join.
		var ips []ipPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case (*dns.A):
				ips = append(ips, ipPort{ip: rr.A, port: defaultPort, nodeName: nodeName})
			case (*dns.AAAA):
				ips = append(ips, ipPort{ip: rr.AAAA, port: defaultPort, nodeName: nodeName})
			case (*dns.CNAME):
				m.logger.Printf("[DEBUG] memberlist: Ignoring CNAME RR in TCP-first answer for '%s'", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

// resolveAddr is used to resolve the address into an address,
// port, and error. If no port is given, use the default
func (m *Members) resolveAddr(hostStr string) ([]ipPort, error) {
	// First peel off any leading node name. This is optional.
	nodeName := ""
	if slashIdx := strings.Index(hostStr, "/"); slashIdx >= 0 {
		if slashIdx == 0 {
			return nil, fmt.Errorf("empty node name provided")
		}
		nodeName = hostStr[0:slashIdx]
		hostStr = hostStr[slashIdx+1:]
	}

	// This captures the supplied port, or the default one.
	hostStr = ensurePort(hostStr, m.config.BindPort)
	host, sport, err := net.SplitHostPort(hostStr)
	if err != nil {
		return nil, err
	}
	lport, err := strconv.ParseUint(sport, 10, 16)
	if err != nil {
		return nil, err
	}
	port := uint16(lport)

	// If it looks like an IP address we are done. The SplitHostPort() above
	// will make sure the host part is in good shape for parsing, even for
	// IPv6 addresses.
	if ip := net.ParseIP(host); ip != nil {
		return []ipPort{
			ipPort{ip: ip, port: port, nodeName: nodeName},
		}, nil
	}

	// First try TCP so we have the best chance for the largest list of
	// hosts to join. If this fails it's not fatal since this isn't a standard
	// way to query DNS, and we have a fallback below.
	ips, err := m.tcpLookupIP(host, port, nodeName)
	if err != nil {
		m.logger.Printf("[DEBUG] memberlist: TCP-first lookup failed for '%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	// If TCP didn't yield anything then use the normal Go resolver which
	// will try UDP, then might possibly try TCP again if the UDP response
	// indicates it was truncated.
	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips = make([]ipPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, ipPort{ip: ip, port: port, nodeName: nodeName})
	}
	return ips, nil
}

// setAlive 用于将此节点标记为活动节点。这就像我们自己的network channel收到一个alive通知一样。
func (m *Members) setAlive() error {
	//TODO 获取广播地址？会一直变么
	addr, port, err := m.refreshAdvertise()
	if err != nil {
		return err
	}

	// 检查是不是IPv4、IPv6地址
	ipAddr, err := sockaddr.NewIPAddr(addr.String())
	if err != nil {
		return fmt.Errorf("解析通信地址失败: %v", err)
	}
	ifAddrs := []sockaddr.IfAddr{
		sockaddr.IfAddr{
			SockAddr: ipAddr,
		},
	}
	// 返回匹配和不匹配的ifaddr列表，其中包含rfc指定的相关特征。
	_, publicIfs, err := sockaddr.IfByRFC("6890", ifAddrs)
	if len(publicIfs) > 0 && !m.config.EncryptionEnabled() {
		m.logger.Printf("[WARN] memberlist: 绑定到公共地址而不加密!")
	}

	// 判断元数据的大小。
	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("节点元数据长度超过限制")
		}
	}

	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name, // 节点名字、唯一
		Addr:        addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&a, nil, true)

	return nil
}

func (m *Members) getAdvertise() (net.IP, uint16) {
	m.advertiseLock.RLock()
	defer m.advertiseLock.RUnlock()
	return m.advertiseAddr, m.advertisePort
}

// 设置广播地址
func (m *Members) setAdvertise(addr net.IP, port int) {
	m.advertiseLock.Lock()
	defer m.advertiseLock.Unlock()
	m.advertiseAddr = addr
	m.advertisePort = uint16(port)
}

// 刷新广播地址
func (m *Members) refreshAdvertise() (net.IP, int, error) {
	addr, port, err := m.transport.FinalAdvertiseAddr(m.config.AdvertiseAddr, m.config.AdvertisePort) // "" 8000
	if err != nil {
		return nil, 0, fmt.Errorf("获取地址失败: %v", err)
	}
	m.setAdvertise(addr, port)
	return addr, port, nil
}

// LocalNode is used to return the local Node
func (m *Members) LocalNode() *Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	state := m.nodeMap[m.config.Name]
	return &state.Node
}

// UpdateNode is used to trigger re-advertising the local node. This is
// primarily used with a Delegate to support dynamic updates to the local
// meta data.  This will block until the update message is successfully
// broadcasted to a member of the cluster, if any exist or until a specified
// timeout is reached.
func (m *Members) UpdateNode(timeout time.Duration) error {
	// Get the node meta data
	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	// Get the existing node
	m.nodeLock.RLock()
	state := m.nodeMap[m.config.Name]
	m.nodeLock.RUnlock()

	// Format a new alive message
	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        state.Addr,
		Port:        state.Port,
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	notifyCh := make(chan struct{})
	m.aliveNode(&a, notifyCh, true)

	// Wait for the broadcast or a timeout
	if m.anyAlive() {
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		select {
		case <-notifyCh:
		case <-timeoutCh:
			return fmt.Errorf("timeout waiting for update broadcast")
		}
	}
	return nil
}

// Deprecated: SendTo is deprecated in favor of SendBestEffort, which requires a node to
// target. If you don't have a node then use SendToAddress.
func (m *Members) SendTo(to net.Addr, msg []byte) error {
	a := Address{Addr: to.String(), Name: ""}
	return m.SendToAddress(a, msg)
}

func (m *Members) SendToAddress(a Address, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	return m.rawSendMsgPacket(a, nil, buf)
}

// Deprecated: SendToUDP is deprecated in favor of SendBestEffort.
func (m *Members) SendToUDP(to *Node, msg []byte) error {
	return m.SendBestEffort(to, msg)
}

// Deprecated: SendToTCP is deprecated in favor of SendReliable.
func (m *Members) SendToTCP(to *Node, msg []byte) error {
	return m.SendReliable(to, msg)
}

// SendBestEffort uses the unreliable packet-oriented interface of the transport
// to target a user message at the given node (this does not use the gossip
// mechanism). The maximum size of the message depends on the configured
// UDPBufferSize for this memberlist instance.
func (m *Members) SendBestEffort(to *Node, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	a := Address{Addr: to.Address(), Name: to.Name}
	return m.rawSendMsgPacket(a, to, buf)
}

// SendReliable uses the reliable stream-oriented interface of the transport to
// target a user message at the given node (this does not use the gossip
// mechanism). Delivery is guaranteed if no error is returned, and there is no
// limit on the size of the message.
func (m *Members) SendReliable(to *Node, msg []byte) error {
	return m.sendUserMsg(to.FullAddress(), msg)
}

// Members returns a list of all known live nodes. The node structures
// returned must not be modified. If you wish to modify a Node, make a
// copy first.
func (m *Members) Members() []*Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

// NumMembers returns the number of alive nodes currently known. Between
// the time of calling this and calling Members, the number of alive nodes
// may have changed, so this shouldn't be used to determine how many
// members will be returned by Members.
func (m *Members) NumMembers() (alive int) {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			alive++
		}
	}

	return
}

// Leave will broadcast a leave message but will not shutdown the background
// listeners, meaning the node will continue participating in gossip and state
// updates.
//
// This will block until the leave message is successfully broadcasted to
// a member of the cluster, if any exist or until a specified timeout
// is reached.
//
// This method is safe to call multiple times, but must not be called
// after the cluster is already shut down.
func (m *Members) Leave(timeout time.Duration) error {
	m.leaveLock.Lock()
	defer m.leaveLock.Unlock()

	if m.hasShutdown() {
		panic("leave after shutdown")
	}

	if !m.hasLeft() {
		atomic.StoreInt32(&m.leave, 1)

		m.nodeLock.Lock()
		state, ok := m.nodeMap[m.config.Name]
		m.nodeLock.Unlock()
		if !ok {
			m.logger.Printf("[WARN] memberlist: Leave but we're not in the node map.")
			return nil
		}

		// This dead message is special, because Node and From are the
		// same. This helps other nodes figure out that a node left
		// intentionally. When Node equals From, other nodes know for
		// sure this node is gone.
		d := dead{
			Incarnation: state.Incarnation,
			Node:        state.Name,
			From:        state.Name,
		}
		m.deadNode(&d)

		// Block until the broadcast goes out
		if m.anyAlive() {
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case <-m.leaveBroadcast:
			case <-timeoutCh:
				return fmt.Errorf("timeout waiting for leave broadcast")
			}
		}
	}

	return nil
}

// Check for any other alive node.
func (m *Members) anyAlive() bool {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	for _, n := range m.nodes {
		if !n.DeadOrLeft() && n.Name != m.config.Name {
			return true
		}
	}
	return false
}

// GetHealthScore gives this instance's idea of how well it is meeting the soft
// real-time requirements of the protocol. Lower numbers are better, and zero
// means "totally healthy".
func (m *Members) GetHealthScore() int {
	return m.awareness.GetHealthScore()
}

// ProtocolVersion returns the 协议版本 currently in use by
// this memberlist.
func (m *Members) ProtocolVersion() uint8 {
	// NOTE: This method exists so that in the future we can control
	// any locking if necessary, if we change the 协议版本 at
	// runtime, etc.
	return m.config.ProtocolVersion
}

// Shutdown 优雅的退出集群、发送Leave消息【幂等】
func (m *Members) Shutdown() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()
	// 之前为0
	if m.hasShutdown() {
		return nil
	}
	// 设置为1
	if err := m.transport.Shutdown(); err != nil {
		m.logger.Printf("[错误] 停止transport: %v", err)
	}

	atomic.StoreInt32(&m.shutdown, 1) // 设置为1 ;执行了两次
	close(m.shutdownCh)
	m.deschedule() // 停止定时器
	return nil
}

func (m *Members) hasShutdown() bool {
	return atomic.LoadInt32(&m.shutdown) == 1
}

func (m *Members) hasLeft() bool {
	return atomic.LoadInt32(&m.leave) == 1
}

func (m *Members) getNodeState(addr string) NodeStateType {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.State
}

func (m *Members) getNodeStateChange(addr string) time.Time {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.StateChange
}

func (m *Members) changeNode(addr string, f func(*nodeState)) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	n := m.nodeMap[addr]
	f(n)
}
