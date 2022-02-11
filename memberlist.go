// Package memberlist /*
package memberlist

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist/broadcast_tree"
	"github.com/hashicorp/memberlist/pkg"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	sockAddr "github.com/hashicorp/go-sockaddr"
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
	Shutdown       int32 // 停止标志
	ShutdownCh     chan struct{}
	leave          int32
	LeaveBroadcast chan struct{} // 离开广播

	ShutdownLock sync.Mutex
	leaveLock    sync.Mutex

	Transport            NodeAwareTransport
	HandoffCh            chan struct{} // 通知有待处理的信息
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

	Broadcasts *broadcast_tree.TransmitLimitedQueue

	Logger *log.Logger
}

// SetAlive 用于将此节点标记为活动节点。这就像我们自己的network channel收到一个Alive通知一样。
func (m *Members) SetAlive() error {
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
	m.AliveNode(&a, nil, true) // 存储节点state,广播自己存活消息

	return nil
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

// Leave will broadcast a leave message but will not Shutdown the background
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

	if m.hasSetShutdown() {
		panic("在停止后离开")
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

		// 阻止直到广播出去、或者超时
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

// ------------------------------------------ OVER ------------------------------------------------------------

func (m *Members) hasSetShutdown() bool {
	return atomic.LoadInt32(&m.Shutdown) == 1
}

func (m *Members) hasLeft() bool {
	return atomic.LoadInt32(&m.leave) == 1
}

// ProtocolVersion 返回当前的协议版本
func (m *Members) ProtocolVersion() uint8 {
	return m.Config.ProtocolVersion // 2
}

// EncryptionVersion 返回加密版本
func (m *Members) EncryptionVersion() EncryptionVersion {
	switch m.ProtocolVersion() { //2
	case 1:
		return 0
	default:
		return 1
	}
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

// SetShutdown 优雅的退出集群、发送Leave消息【幂等】
func (m *Members) SetShutdown() error {
	m.ShutdownLock.Lock()
	defer m.ShutdownLock.Unlock()
	// 之前为0
	if m.hasSetShutdown() {
		return nil
	}
	// 设置为1
	if err := m.Transport.SetShutdown(); err != nil {
		m.Logger.Printf("[错误] 停止Transport: %v", err)
	}

	atomic.StoreInt32(&m.Shutdown, 1) // 设置为1 ;执行了两次
	close(m.ShutdownCh)
	m.deschedule() // 停止定时器
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

// ResolveAddr 解析hostStr、可以是域名 ,返回IpPort
func (m *Members) ResolveAddr(hostStr string) ([]pkg.IpPort, error) {
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
		return []pkg.IpPort{
			pkg.IpPort{IP: ip, Port: port, NodeName: nodeName},
		}, nil
	}
	// 尝试使用tcp 解析
	ips, err := pkg.TcpLookupIP(host, port, nodeName, m.Config.DNSConfigPath)
	if err != nil {
		m.Logger.Printf("[DEBUG] memberlist: TCP-first lookup 失败'%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	return pkg.UdpLookupIP(host, port, nodeName)

}

// Join  加入（Join）是用来获取一个现有的成员，并试图通过联系所有给定的主机和执行状态同步来加入一个集群。
// 最初，成员只包含我们自己的状态，所以这样做将导致远程节点警觉到这个节点的存在，有效地加入集群。
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
			a := pkg.Address{Addr: hp, Name: Addr.NodeName}
			if err := m.PushPullNode(a, true); err != nil {
				err = fmt.Errorf("加入失败 %s: %v", a.Addr, err)
				errs = multierror.Append(errs, err)
				m.Logger.Printf("[DEBUG] memberlist: %v", err)
				continue
			} // 建立tcp 链接
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

// SendToAddress UDP发送UserMsg
func (m *Members) SendToAddress(a pkg.Address, msg []byte) error {
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(UserMsg)
	buf = append(buf, msg...)
	return m.RawSendMsgPacket(a, nil, buf)
}

// SendBestEffort UDP发送UserMsg
func (m *Members) SendBestEffort(to *Node, msg []byte) error {
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(UserMsg)
	buf = append(buf, msg...)

	a := pkg.Address{Addr: to.Address(), Name: to.Name}
	return m.RawSendMsgPacket(a, to, buf)
}

// SendToUDP UDP发送UserMsg
func (m *Members) SendToUDP(to *Node, msg []byte) error {
	return m.SendBestEffort(to, msg)
}
