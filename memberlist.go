// Package memberlist /*
package memberlist

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"github.com/hashicorp/memberlist/queue_broadcast"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	sockAddr "github.com/hashicorp/go-sockaddr"
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
	SequenceNum uint32 // 本地序列号
	// 周期性的full state sync，使用incarnation number去调协
	incarnation uint32 // Local incarnation number
	numNodes    uint32 // 已知节点数(估计)
	PushPullReq uint32 // push/pull 请求数

	advertiseLock sync.RWMutex
	advertiseAddr net.IP
	advertisePort uint16

	Config         *Config
	shutdown       int32 // 停止标志
	ShutdownCh     chan struct{}
	leave          int32
	LeaveBroadcast chan struct{} // 离开广播

	shutdownLock sync.Mutex
	leaveLock    sync.Mutex

	Transport            NodeAwareTransport
	HandoffCh            chan struct{} //TODO 消息队列
	HighPriorityMsgQueue *list.List
	LowPriorityMsgQueue  *list.List
	msgQueueLock         sync.Mutex

	NodeLock   sync.RWMutex
	probeIndex int                   // 节点探活索引  与nodes对应
	Nodes      []*NodeState          // Known Nodes
	NodeMap    map[string]*NodeState // ls-2018.local -> NodeState
	NodeTimers map[string]*Suspicion // ls-2018.local -> Suspicion timer
	Awareness  *pkg.Awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTickCh chan struct{}

	AckLock     sync.Mutex
	AckHandlers map[uint32]*AckHandler

	Broadcasts *queue_broadcast.TransmitLimitedQueue

	Logger *log.Logger
}

// Join is used to take an existing Members and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Members only contains our own state, so doing this will cause
// remote Nodes to become aware of the existence of this node, effectively
// joining the cluster.
//
// This returns the number of hosts successfully contacted and an error if
// none could be reached. If an error is returned, the node did not successfully
// join the cluster.
// 加入（Join）是用来获取一个现有的成员，并试图通过联系所有给定的主机和执行状态同步来加入一个集群。
// 最初，成员只包含我们自己的状态，所以这样做将导致远程节点意识到这个节点的存在，有效地加入集群。
// 这将返回成功联系到的主机的数量，如果没有联系到，则返回错误。如果返回错误，说明该节点没有成功加入集群。
func (m *Members) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		Addrs, err := m.ResolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("解析地址失败 %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			m.Logger.Printf("[WARN] memberlist: %v", err)
			continue
		}

		for _, Addr := range Addrs {
			hp := pkg.JoinHostPort(Addr.IP.String(), Addr.Port)
			a := Address{Addr: hp, Name: Addr.NodeName}
			if err := m.PushPullNode(a, true); err != nil {
				err = fmt.Errorf("加入失败 %s: %v", a.Addr, err)
				errs = multierror.Append(errs, err)
				m.Logger.Printf("[DEBUG] memberlist: %v", err)
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

// IpPort 希望加入的节点信息
type IpPort struct {
	IP       net.IP
	Port     uint16
	NodeName string // optional
}

// tcpLookupIP 是一个辅助工具，用于启动对指定主机的基于TCP的DNS查询。
// 内置的Go解析器将首先进行UDP查询，只有在响应设置了truncate bit时才会使用TCP，这在像Consul的DNS服务器上并不常见。
// 通过直接进行TCP查询，我们得到了最大的主机列表加入的最佳机会。由于加入是相对罕见的事件，所以做这个相当昂贵的操作是可以的。
func (m *Members) tcpLookupIP(host string, defaultPort uint16, nodeName string) ([]IpPort, error) {
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
	cc, err := dns.ClientConfigFromFile(m.Config.DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {
		// We support host:Port in the DNS Config, but need to add the
		// default Port if one is not supplied.
		server := cc.Servers[0]
		if !pkg.HasPort(server) {
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
		var ips []IpPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case *dns.A:
				ips = append(ips, IpPort{IP: rr.A, Port: defaultPort, NodeName: nodeName})
			case *dns.AAAA:
				ips = append(ips, IpPort{IP: rr.AAAA, Port: defaultPort, NodeName: nodeName})
			case *dns.CNAME:
				m.Logger.Printf("[DEBUG] memberlist: Ignoring CNAME RR in TCP-first answer for '%s'", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

// ResolveAddr is used to resolve the Address into an Address,
// Port, and error. If no Port is given, use the default
func (m *Members) ResolveAddr(hostStr string) ([]IpPort, error) {
	// 首先去掉任何leading节点名称。这是可选的。
	nodeName := ""
	slashIdx := strings.Index(hostStr, "/") // 127.0.0.1:8000       -1
	if slashIdx >= 0 {
		if slashIdx == 0 {
			return nil, fmt.Errorf("empty node name provided")
		}
		nodeName = hostStr[0:slashIdx]
		hostStr = hostStr[slashIdx+1:]
	}

	// 这将捕获所提供的端口，或默认的端口。
	hostStr = pkg.EnsurePort(hostStr, m.Config.BindPort) // 8000
	host, sport, err := net.SplitHostPort(hostStr)
	if err != nil {
		return nil, err
	}
	lport, err := strconv.ParseUint(sport, 10, 16)
	if err != nil {
		return nil, err
	}
	port := uint16(lport)

	if ip := net.ParseIP(host); ip != nil {
		return []IpPort{
			IpPort{IP: ip, Port: port, NodeName: nodeName},
		}, nil
	}
	// 尝试使用tcp 解析
	ips, err := m.tcpLookupIP(host, port, nodeName)
	if err != nil {
		m.Logger.Printf("[DEBUG] memberlist: TCP-first lookup 失败'%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	// 尝试使用udp 解析
	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips = make([]IpPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, IpPort{IP: ip, Port: port, NodeName: nodeName})
	}
	return ips, nil
}

// SetAlive 用于将此节点标记为活动节点。这就像我们自己的network channel收到一个Alive通知一样。
func (m *Members) SetAlive() error {
	//TODO 获取广播地址？会一直变么
	Addr, port, err := m.RefreshAdvertise()
	if err != nil {
		return err
	}

	// 检查是不是IPv4、IPv6地址
	ipAddr, err := sockAddr.NewIPAddr(Addr.String())
	if err != nil {
		return fmt.Errorf("解析通信地址失败: %v", err)
	}
	ifAddrs := []sockAddr.IfAddr{
		sockAddr.IfAddr{
			SockAddr: ipAddr,
		},
	}
	// 返回匹配和不匹配的ifAddr列表，其中包含rfc指定的相关特征。
	_, publicIfs, err := sockAddr.IfByRFC("6890", ifAddrs)
	if len(publicIfs) > 0 && !m.Config.EncryptionEnabled() {
		m.Logger.Printf("[WARN] memberlist: 绑定到公共地址而不加密!")
	}

	// 判断元数据的大小。
	var meta []byte
	if m.Config.Delegate != nil {
		meta = m.Config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("节点元数据长度超过限制")
		}
	}

	a := Alive{
		Incarnation: m.NextIncarnation(), // 1 周期性的full state sync，使用incarnation number去调协
		Node:        m.Config.Name,       // 节点名字、唯一
		Addr:        Addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&a, nil, true) // 存储节点state,广播存活消息

	return nil
}

//OK
func (m *Members) getAdvertise() (net.IP, uint16) {
	m.advertiseLock.RLock()
	defer m.advertiseLock.RUnlock()
	return m.advertiseAddr, m.advertisePort
}

// 设置广播地址
func (m *Members) setAdvertise(Addr net.IP, port int) {
	m.advertiseLock.Lock()
	defer m.advertiseLock.Unlock()
	m.advertiseAddr = Addr
	m.advertisePort = uint16(port)
}

// RefreshAdvertise 刷新广播地址
func (m *Members) RefreshAdvertise() (net.IP, int, error) {
	Addr, port, err := m.Transport.FinalAdvertiseAddr(m.Config.AdvertiseAddr, m.Config.AdvertisePort) // "" 8000
	fmt.Println("RefreshAdvertise [sockAddr.GetPrivateIP] ---->", Addr, port)
	if err != nil {
		return nil, 0, fmt.Errorf("获取地址失败: %v", err)
	}
	m.setAdvertise(Addr, port)
	return Addr, port, nil
}

func (m *Members) SendToAddress(a Address, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(UserMsg)
	buf = append(buf, msg...)

	// Send the message
	return m.RawSendMsgPacket(a, nil, buf)
}

// Deprecated: SendToUDP is deprecated in favor of SendBestEffort.
func (m *Members) SendToUDP(to *Node, msg []byte) error {
	return m.SendBestEffort(to, msg)
}

// Deprecated: SendToTCP is deprecated in favor of SendReliable.
func (m *Members) SendToTCP(to *Node, msg []byte) error {
	return m.SendReliable(to, msg)
}

// SendBestEffort uses the unreliable packet-oriented interface of the Transport
// to target a user message at the given node (this does not use the gossip
// mechanism). The maximum size of the message depends on the configured
// UDPBufferSize for this memberlist instance.
func (m *Members) SendBestEffort(to *Node, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(UserMsg)
	buf = append(buf, msg...)

	// Send the message
	a := Address{Addr: to.Address(), Name: to.Name}
	return m.RawSendMsgPacket(a, to, buf)
}

// SendReliable uses the reliable stream-oriented interface of the Transport to
// target a user message at the given node (this does not use the gossip
// mechanism). Delivery is guaranteed if no error is returned, and there is no
// limit on the size of the message.
func (m *Members) SendReliable(to *Node, msg []byte) error {
	return m.sendUserMsg(to.FullAddress(), msg)
}

// Deprecated: SendTo is deprecated in favor of SendBestEffort, which requires a node to
// target. If you don't have a node then use SendToAddress.
func (m *Members) SendTo(to net.Addr, msg []byte) error {
	a := Address{Addr: to.String(), Name: ""}
	return m.SendToAddress(a, msg)
}

// NumMembers 返回当前已知的存活结点
func (m *Members) NumMembers() (Alive int) {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()

	for _, n := range m.Nodes {
		if !n.DeadOrLeft() {
			Alive++
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

		m.NodeLock.Lock()
		state, ok := m.NodeMap[m.Config.Name]
		m.NodeLock.Unlock()
		if !ok {
			m.Logger.Printf("[WARN] memberlist: Leave but we're not in the node map.")
			return nil
		}

		// This Dead message is special, because Node and From are the
		// same. This helps other Nodes figure out that a node left
		// intentionally. When Node equals From, other Nodes know for
		// sure this node is gone.
		d := Dead{
			Incarnation: state.Incarnation,
			Node:        state.Name,
			From:        state.Name,
		}
		m.DeadNode(&d)

		// 阻止直到广播出去
		if m.anyAlive() {
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case <-m.LeaveBroadcast:
			case <-timeoutCh:
				return fmt.Errorf("timeout waiting for leave broadcast")
			}
		}
	}

	return nil
}

// 检查是否有存活结点、非本机
func (m *Members) anyAlive() bool {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()
	for _, n := range m.Nodes {
		if !n.DeadOrLeft() && n.Name != m.Config.Name {
			return true
		}
	}
	return false
}

// GetHealthScore 节点的健康程度 数字越小越好，而零意味着 "完全健康"。
func (m *Members) GetHealthScore() int {
	return m.Awareness.GetHealthScore()
}

// ProtocolVersion 返回当前的协议版本
func (m *Members) ProtocolVersion() uint8 {
	return m.Config.ProtocolVersion // 2
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
	if err := m.Transport.Shutdown(); err != nil {
		m.Logger.Printf("[错误] 停止Transport: %v", err)
	}

	atomic.StoreInt32(&m.shutdown, 1) // 设置为1 ;执行了两次
	close(m.ShutdownCh)
	m.deschedule() // 停止定时器
	return nil
}

func (m *Members) hasShutdown() bool {
	return atomic.LoadInt32(&m.shutdown) == 1
}

func (m *Members) hasLeft() bool {
	return atomic.LoadInt32(&m.leave) == 1
}
