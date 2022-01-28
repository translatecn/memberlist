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
	//
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

	// RetransmitMult is the multiplier for the number of retransmissions
	// that are attempted for messages broadcasted over gossip. The actual
	// count of retransmissions is calculated using the formula:
	//
	//   Retransmits = RetransmitMult * log(N+1)
	//
	// This allows the retransmits to scale properly with cluster size. The
	// higher the multiplier, the more likely a failed broadcast is to converge
	// at the expense of increased bandwidth.
	RetransmitMult int

	// SuspicionMult is the multiplier for determining the time an
	// inaccessible node is considered suspect before declaring it dead.
	// The actual timeout is calculated using the formula:
	//
	//   SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval
	//
	// This allows the timeout to scale properly with expected propagation
	// delay with a larger cluster size. The higher the multiplier, the longer
	// an inaccessible node is considered part of the cluster before declaring
	// it dead, giving that suspect node more time to refute if it is indeed
	// still alive.
	SuspicionMult int

	// SuspicionMaxTimeoutMult is the multiplier applied to the
	// SuspicionTimeout used as an upper bound on detection time. This max
	// timeout is calculated using the formula:
	//
	// SuspicionMaxTimeout = SuspicionMaxTimeoutMult * SuspicionTimeout
	//
	// If everything is working properly, confirmations from other nodes will
	// accelerate suspicion timers in a manner which will cause the timeout
	// to reach the base SuspicionTimeout before that elapses, so this value
	// will typically only come into play if a node is experiencing issues
	// communicating with other nodes. It should be set to a something fairly
	// large so that a node having problems will have a lot of chances to
	// recover before falsely declaring other nodes as failed, but short
	// enough for a legitimately isolated node to still make progress marking
	// nodes failed in a reasonable amount of time.
	SuspicionMaxTimeoutMult int

	// PushPullInterval is the interval between complete state syncs.
	// Complete state syncs are done with a single node over TCP and are
	// quite expensive relative to standard gossiped messages. Setting this
	// to zero will disable state push/pull syncs completely.
	//
	// Setting this interval lower (more frequent) will increase convergence
	// speeds across larger clusters at the expense of increased bandwidth
	// usage.
	PushPullInterval time.Duration

	// ProbeInterval and ProbeTimeout are used to configure probing
	// behavior for memberlist.
	//
	// ProbeInterval is the interval between random node probes. Setting
	// this lower (more frequent) will cause the memberlist cluster to detect
	// failed nodes more quickly at the expense of increased bandwidth usage.
	//
	// ProbeTimeout is the timeout to wait for an ack from a probed node
	// before assuming it is unhealthy. This should be set to 99-percentile
	// of RTT (round-trip time) on your network.
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	// DisableTcpPings will turn off the fallback TCP pings that are attempted
	// if the direct UDP ping fails. These get pipelined along with the
	// indirect UDP pings.
	DisableTcpPings bool

	// DisableTcpPingsForNode is like DisableTcpPings, but lets you control
	// whether to perform TCP pings on a node-by-node basis.
	DisableTcpPingsForNode func(nodeName string) bool

	// AwarenessMaxMultiplier will increase the probe interval if the node
	// becomes aware that it might be degraded and not meeting the soft real
	// time requirements to reliably probe other nodes.
	AwarenessMaxMultiplier int

	// GossipInterval and GossipNodes are used to configure the gossip
	// behavior of memberlist.
	//
	// GossipInterval is the interval between sending messages that need
	// to be gossiped that haven't been able to piggyback on probing messages.
	// If this is set to zero, non-piggyback gossip is disabled. By lowering
	// this value (more frequent) gossip messages are propagated across
	// the cluster more quickly at the expense of increased bandwidth.
	//
	// GossipNodes is the number of random nodes to send gossip messages to
	// per GossipInterval. Increasing this number causes the gossip messages
	// to propagate across the cluster more quickly at the expense of
	// increased bandwidth.
	//
	// GossipToTheDeadTime is the interval after which a node has died that
	// we will still try to gossip to it. This gives it a chance to refute.
	GossipInterval      time.Duration
	GossipNodes         int
	GossipToTheDeadTime time.Duration

	// GossipVerifyIncoming controls whether to enforce encryption for incoming
	// gossip. It is used for upshifting from unencrypted to encrypted gossip on
	// a running cluster.
	GossipVerifyIncoming bool

	// GossipVerifyOutgoing controls whether to enforce encryption for outgoing
	// gossip. It is used for upshifting from unencrypted to encrypted gossip on
	// a running cluster.
	GossipVerifyOutgoing bool

	// EnableCompression is used to control message compression. This can
	// be used to reduce bandwidth usage at the cost of slightly more CPU
	// utilization. This is only available starting at 协议版本 1.
	EnableCompression bool

	// SecretKey 加密秘钥
	SecretKey []byte

	// 钥匙环存放着内部使用的所有加密钥匙。它使用SecretKey和SecretKeys值自动进行初始化。
	Keyring *Keyring

	// Delegate and Events are delegates for receiving and providing
	// data to memberlist via callback mechanisms. For Delegate, see
	// the Delegate interface. For Events, see the EventDelegate interface.
	//
	// The DelegateProtocolMin/Max are used to guarantee protocol-compatibility
	// for any custom messages that the delegate might do (broadcasts,
	// local/remote state, etc.). If you don't set these, then the protocol
	// versions will just be zero, and version compliance won't be done.
	Delegate                Delegate
	DelegateProtocolVersion uint8
	DelegateProtocolMin     uint8
	DelegateProtocolMax     uint8
	Events                  EventDelegate
	Conflict                ConflictDelegate
	Merge                   MergeDelegate
	Ping                    PingDelegate
	Alive                   AliveDelegate

	// DNSConfigPath points to the system's DNS config file, usually located
	// at /etc/resolv.conf. It can be overridden via config for easier testing.
	DNSConfigPath string

	// LogOutput is the writer where logs should be sent. If this is not
	// set, logging will go to stderr by default. You cannot specify both LogOutput
	// and Logger at the same time.
	LogOutput io.Writer

	// Logger is a custom logger which you provide. If Logger is set, it will use
	// this for the internal logger. If Logger is not set, it will fall back to the
	// behavior for using LogOutput. You cannot specify both LogOutput and Logger
	// at the same time.
	Logger *log.Logger

	// Size of Memberlist's internal channel which handles UDP messages. The
	// size of this determines the size of the queue which Memberlist will keep
	// while UDP messages are handled.
	HandoffQueueDepth int

	// Maximum number of bytes that memberlist will put in a packet (this
	// will be for UDP packets by default with a NetTransport). A safe value
	// for this is typically 1400 bytes (which is the default). However,
	// depending on your network's MTU (Maximum Transmission Unit) you may
	// be able to increase this to get more content into each gossip packet.
	// This is a legacy name for backward compatibility but should really be
	// called PacketBufferSize now that we have generalized the transport.
	UDPBufferSize int

	// DeadNodeReclaimTime controls the time before a dead node's name can be
	// reclaimed by one with a different address or port. By default, this is 0,
	// meaning nodes cannot be reclaimed this way.
	DeadNodeReclaimTime time.Duration

	// RequireNodeNames controls if the name of a node is required when sending
	// a message to that node.
	RequireNodeNames bool
	// CIDRsAllowed If nil, allow any connection (default), otherwise specify all networks
	// allowed to connect (you must specify IPv6/IPv4 separately)
	// Using [] will block all connections.
	CIDRsAllowed []net.IPNet
}

// ParseCIDRs return a possible empty list of all Network that have been parsed
// In case of error, it returns succesfully parsed CIDRs and the last error found
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

// DefaultLANConfig returns a sane set of configurations for Memberlist.
// It uses the hostname as the node name, and otherwise sets very conservative
// values that are sane for most LAN environments. The default configuration
// errs on the side of caution, choosing values that are optimized
// for higher convergence at the cost of higher bandwidth usage. Regardless,
// these values are a good starting point when getting started with memberlist.
func DefaultLANConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Name:                    hostname,
		BindAddr:                "0.0.0.0",
		BindPort:                7946,
		AdvertiseAddr:           "",
		AdvertisePort:           7946,
		ProtocolVersion:         ProtocolVersion2Compatible,
		TCPTimeout:              10 * time.Second,       // Timeout after 10 seconds
		IndirectChecks:          3,                      // Use 3 nodes for the indirect ping
		RetransmitMult:          4,                      // Retransmit a message 4 * log(N+1) nodes
		SuspicionMult:           4,                      // Suspect a node for 4 * log(N+1) * Interval
		SuspicionMaxTimeoutMult: 6,                      // For 10k nodes this will give a max timeout of 120 seconds
		PushPullInterval:        30 * time.Second,       // Low frequency
		ProbeTimeout:            500 * time.Millisecond, // Reasonable RTT time for LAN
		ProbeInterval:           1 * time.Second,        // Failure check every second
		DisableTcpPings:         false,                  // TCP pings are safe, even with mixed versions
		AwarenessMaxMultiplier:  8,                      // Probe interval backs off to 8 seconds

		GossipNodes:          3,                      // Gossip to 3 nodes
		GossipInterval:       200 * time.Millisecond, // Gossip more rapidly
		GossipToTheDeadTime:  30 * time.Second,       // Same as push/pull
		GossipVerifyIncoming: true,
		GossipVerifyOutgoing: true,

		EnableCompression: true, // Enable compression by default

		SecretKey: nil,
		Keyring:   nil,

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

// IPMustBeChecked return true if IPAllowed must be called
func (c *Config) IPMustBeChecked() bool {
	return len(c.CIDRsAllowed) > 0
}

// IPAllowed return an error if access to memberlist is denied
func (c *Config) IPAllowed(ip net.IP) error {
	if !c.IPMustBeChecked() {
		return nil
	}
	for _, n := range c.CIDRsAllowed {
		if n.Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("%s is not allowed", ip)
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
