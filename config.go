package memberlist

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	multierror "github.com/hashicorp/go-multierror"
)

type Config struct {
	// 此节点的名称。这在集群中必须是唯一的。
	Name string

	// Transport
	// 是一种钩子，用于提供与其他节点通信的自定义代码。
	// 如果这是空的，那么成员列表将在默认情况下使用这个结构的BindAddr和BindPort创建一个NetTransport。
	Transport Transport

	// Label 是一个可选的字节集，要包含在每个包和流的外部。
	// 如果gossip启用了加密，并且设置了这个值，则将其视为经过GCM认证的数据。
	Label string

	// SkipInboundLabelCheck 跳过检查入站数据包和gossip stream 是否需要有标签前缀。
	SkipInboundLabelCheck bool

	// 绑定的地址和要监听的端口。该端口同时用于UDP和TCP 。假设其他节点在此端口上运行，但它们不需要这样做。
	BindAddr string
	BindPort int

	// 向其他集群成员发布的地址。
	AdvertiseAddr string
	AdvertisePort int

	// ProtocolVersion 通信协议版本
	ProtocolVersion uint8

	// TCPTimeout 与远程节点建立流连接的超时时间。
	TCPTimeout time.Duration

	// IndirectChecks
	//todo 在直接探测失败的情况下，将请求执行对节点的间接探测的节点数。
	IndirectChecks int

	// RetransmitMult 是对通过gossip广播的信息尝试重传的倍数。实际的重传次数是用公式计算的。
	//   Retransmits = RetransmitMult * log(N+1)
	// 这使得重传可以随着集群的大小而适当扩展。倍数越高，失败的消息就越有可能以增加带宽为代价。
	RetransmitMult int

	// SuspicionMult 是确定无法访问的节点在宣布其死亡之前被认为是可疑的时间的乘数。
	// 实际的超时是用公式计算的。
	//   SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval
	// 这允许超时与预期的传播延迟在较大的集群规模下适当扩展。
	// 乘数越高，在宣布一个无法访问的节点死亡之前，它被认为是集群的一部分的时间就越长，
	// 如果这个可疑的节点确实还活着，就有更多的时间来反驳。
	SuspicionMult int

	// SuspicionMaxTimeoutMult 是应用于SuspicionTimeout的乘数，作为检测时间的上限。
	// 这个最大的超时时间是用公式计算的。
	// SuspicionMaxTimeout = SuspicionMaxTimeoutMult * SuspicionTimeoutv
	// 如果一切工作正常，来自其他节点的确认将加速怀疑计时器，这将导致超时在其结束前达到基本SuspicionTimeout，
	// 所以这个值通常只在一个节点遇到与其他节点通信的问题时才会发挥作用。
	// 它应该被设置为一个相当大的值，以便出现问题的节点在错误地宣布其他节点失败之前有很多机会恢复，
	// 但又足够短，以便合法隔离的节点仍然能够在合理的时间内取得进展，标记节点失败。
	SuspicionMaxTimeoutMult int

	// 是完整状态同步的间隔时间。
	PushPullInterval time.Duration

	// 随机节点探活;设置得更低（更频繁）将导致成员列表集群更快地检测到故障节点，代价是增加带宽使用。
	ProbeInterval time.Duration
	// 是指在假定被探测的节点不健康之前，等待被探测的节点发出Ack的超时。
	// 这应该被设置为你的网络上RTT（往返时间）的第99百分位数。你的网络上的RTT（往返时间）。
	ProbeTimeout time.Duration

	// 当UDP ping失败时,关闭TCP ping ;控制所有
	DisableTcpPings bool

	// 当UDP ping失败时,关闭TCP ping ;控制单个节点
	DisableTcpPingsForNode func(nodeName string) bool `json:"-"`

	// AwarenessMaxMultiplier
	// 当节点意识到自己可能降级，无法满足可靠探测其他节点的软实时要求时，会增加探测间隔。
	AwarenessMaxMultiplier int

	// GossipInterval 节点间消息探测的间隔
	GossipInterval time.Duration
	// 每个GossipInterval内，将gossip消息随机发送到几个节点. 增加它会使消息同步的越快，但是会增加带宽
	GossipNodes int

	// GossipToTheDeadTime node失败后，继续探测的时间
	GossipToTheDeadTime time.Duration

	// 控制是否对gossip进行加密。它用于在运行的集群上从未加密的gossip转移到加密的gossip。
	GossipVerifyIncoming bool
	GossipVerifyOutgoing bool // 校验出流量; 用于出去的数据加密

	// 是否启用消息压缩
	EnableCompression bool

	// SecretKey 加密秘钥
	SecretKey []byte

	// 钥匙环存放着内部使用的所有加密钥匙。它使用SecretKey和SecretKeys值自动进行初始化。
	Keyring *Keyring

	// Delegate和Events是通过回调机制接收和提供数据给memberlist的。
	DelegateProtocolVersion uint8
	DelegateProtocolMin     uint8
	DelegateProtocolMax     uint8
	Events                  EventDelegate

	// 以下五个都没有初始化
	Delegate                Delegate // 委托
	Conflict ConflictDelegate // 配置 委托/实现
	Merge    MergeDelegate    // 合并 委托/实现
	Ping     PingDelegate     // ping 委托/实现
	Alive    AliveDelegate    // 探活 委托/实现

	// dns 配置文件
	DNSConfigPath string

	LogOutput io.Writer

	Logger *log.Logger

	// UDP消息队列,取决于消息的大小
	HandoffQueueDepth int

	// 写缓冲大小
	UDPBufferSize int

	// 控制一个死亡节点的名字可以被不同地址或端口的节点回收的时间。默认情况下，该值为0，意味着节点不能以这种方式被回收。
	DeadNodeReclaimTime time.Duration

	// RequireNodeNames 控制在发送一个消息到节点时,是否需要节点的名称。
	RequireNodeNames bool // 默认不需要

	// CIDRsAllowed nil,允许所有链接,[]拒绝所有链接
	CIDRsAllowed []net.IPNet
}

// ParseCIDRs 解析CIDR 列表  【192.0.2.1/24】
func ParseCIDRs(v []string) ([]net.IPNet, error) {
	nets := make([]net.IPNet, 0)
	if v == nil {
		return nets, nil
	}
	var errs error
	hasErrors := false
	for _, p := range v {
		_, net, err := net.ParseCIDR(strings.TrimSpace(p))
		if err != nil {
			err = fmt.Errorf("invalid cidr: %s", p)
			errs = multierror.Append(errs, err)
			hasErrors = true
		} else {
			nets = append(nets, *net)
		}
	}
	if !hasErrors {
		errs = nil
	}
	return nets, errs
}

// DefaultLANConfig
// 为Memberlist返回一套合理的配置。它使用主机名作为节点名称，除此之外，还设置了非常保守的值，
// 对大多数局域网环境来说是合理的。默认配置偏向于谨慎，选择了以更高带宽使用为代价的优化值，以实现更高的收敛性。不管怎么说，在开始使用成员列表时，这些值是一个很好的出发点。
func DefaultLANConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Name:                    hostname,
		BindAddr:                "0.0.0.0",
		BindPort:                7946,
		AdvertiseAddr:           "",
		AdvertisePort:           7946,
		ProtocolVersion:         ProtocolVersion2Compatible,
		TCPTimeout:              10 * time.Second,
		IndirectChecks:          3,                      // 使用3个节点进行间接Ping
		RetransmitMult:          4,                      // 转发消息至4 * log(N+1) 个节点
		SuspicionMult:           4,                      // 节点不可靠4 * log(N+1) * Interval
		SuspicionMaxTimeoutMult: 6,                      // 对于超大集群、会给一个最大的超时时间120s
		PushPullInterval:        30 * time.Second,       // 低频
		ProbeTimeout:            500 * time.Millisecond, // 可以响应的往返时间
		ProbeInterval:           1 * time.Second,        // 每一秒进行失败检查
		DisableTcpPings:         false,                  // TCP ping是安全的，即使有混合版本
		AwarenessMaxMultiplier:  8,                      //  探测间隔退至8秒

		GossipNodes:          3,                      // 集群有3个节点
		GossipInterval:       200 * time.Millisecond, // 传播事件
		GossipToTheDeadTime:  30 * time.Second,       // 往返时间
		GossipVerifyIncoming: true,                   // 验证入栈流量
		GossipVerifyOutgoing: true,                   // 验证出站流量

		EnableCompression: true, // 允许压缩

		SecretKey: nil, // 秘钥
		Keyring:   nil, // 秘钥环

		DNSConfigPath: "/etc/resolv.conf",

		HandoffQueueDepth: 1024,
		UDPBufferSize:     1400,
		CIDRsAllowed:      nil, // same as allow all
	}
}

// DefaultWANConfig 的工作原理与DefaultConfig类似，但它会返回一个为广域网环境优化的配置。
// 默认的配置仍然是非常保守的，并在谨慎方面犯了错误。
func DefaultWANConfig() *Config {
	conf := DefaultLANConfig()
	conf.TCPTimeout = 30 * time.Second
	conf.SuspicionMult = 6
	conf.PushPullInterval = 60 * time.Second
	conf.ProbeTimeout = 3 * time.Second
	conf.ProbeInterval = 5 * time.Second
	conf.GossipNodes = 4 // Gossip 的频率较低，但对另外一个节点来说
	conf.GossipInterval = 500 * time.Millisecond
	conf.GossipToTheDeadTime = 60 * time.Second
	return conf
}

// IPMustBeChecked 是否检查CIDR块
func (c *Config) IPMustBeChecked() bool {
	return len(c.CIDRsAllowed) > 0
}

// IPAllowed 检查该IP是否允许    不允许某些IP节点 加入到集群中
func (c *Config) IPAllowed(ip net.IP) error {
	if !c.IPMustBeChecked() {
		return nil
	}
	for _, n := range c.CIDRsAllowed {
		if n.Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("%s 不被允许", ip)
}

// DefaultLocalConfig 的工作原理与DefaultConfig类似，但它会返回一个为本地环回环境优化的配置。
// 默认的配置仍然是非常保守的，并在谨慎方面犯了错误。
func DefaultLocalConfig() *Config {
	conf := DefaultLANConfig()
	conf.TCPTimeout = time.Second
	conf.IndirectChecks = 1
	conf.RetransmitMult = 2
	conf.SuspicionMult = 3
	conf.PushPullInterval = 15 * time.Second
	conf.ProbeTimeout = 200 * time.Millisecond
	conf.ProbeInterval = time.Second
	conf.GossipInterval = 100 * time.Millisecond
	conf.GossipToTheDeadTime = 15 * time.Second
	return conf
}

// EncryptionEnabled 返回是否允许加密
func (c *Config) EncryptionEnabled() bool {
	return c.Keyring != nil && len(c.Keyring.GetKeys()) > 0
}
