package memberlist

import (
	"container/list"
	"fmt"
	"github.com/hashicorp/memberlist/broadcast_tree"
	"github.com/hashicorp/memberlist/pkg"
	"log"
	"os"
	"strings"
)

// NewMembers 创建网络监听器,只能在主线程被调度
func NewMembers(conf *Config) (*Members, error) {
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

	Logger := conf.Logger
	if Logger == nil {
		Logger = log.New(logDest, "", log.LstdFlags)
	}

	// 如果配置中没有给出自定义的网络传输，则默认设置网络传输。
	Transport := conf.Transport // 默认为nil
	if Transport == nil {
		nc := &NetTransportConfig{
			BindAddrs: []string{conf.BindAddr}, // 0.0.0.0
			BindPort:  conf.BindPort,
			Logger:    Logger,
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
					Logger.Printf("[DEBUG] Got bind error: %v", err)
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
			Logger.Printf("[DEBUG] 使用动态绑定端口 %d", port)
		}
		Transport = nt
	}

	nodeAwareTransport, ok := Transport.(NodeAwareTransport)
	if !ok {
		Logger.Printf("[DEBUG] memberlist: 配置的Transport不是一个NodeAwareTransport，一些功能可能无法正常工作。")
		nodeAwareTransport = &ShimNodeAwareTransport{Transport: Transport}
	}

	if len(conf.Label) > LabelMaxSize {
		return nil, fmt.Errorf("不能使用 %q 作为标签: 太长了", conf.Label)
	}

	if conf.Label != "" {
		nodeAwareTransport = &LabelWrappedTransport{
			Label:              conf.Label,
			NodeAwareTransport: nodeAwareTransport,
		}
	}

	m := &Members{
		Config:               conf,
		ShutdownCh:           make(chan struct{}),
		LeaveBroadcast:       make(chan struct{}, 1), //
		Transport:            nodeAwareTransport,
		HandoffCh:            make(chan struct{}, 1),
		HighPriorityMsgQueue: list.New(), // 高优先级消息队列
		LowPriorityMsgQueue:  list.New(), // 低优先级消息队列
		NodeMap:              make(map[string]*NodeState),
		NodeTimers:           make(map[string]*Suspicion),
		Awareness:            pkg.NewAwareness(conf.AwarenessMaxMultiplier), // 感知对象
		AckHandlers:          make(map[uint32]*AckHandler),
		Broadcasts:           &broadcast_tree.TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		Logger:               Logger,
	}
	m.Broadcasts.NumNodes = func() int {
		return m.EstNumNodes()
	}

	// 设置广播地址
	if _, _, err := m.RefreshAdvertise(); err != nil {
		return nil, err
	}

	go m.StreamListen()  // push\pull模式,处理每一个tcp链接 ✅
	go m.PacketListen()  // 从网络中接收消息
	go m.PacketHandler() // 处理消息

	return m, nil
}

// Create 不会链接其他节点、但会开启listeners,允许其他节点加入；之后Config不应该被改变
func Create(conf *Config) (*Members, error) {
	m, err := NewMembers(conf) // ok
	if err != nil {
		return nil, err
	}
	if err := m.SetAlive(); err != nil {
		m.SetShutdown()
		return nil, err
	}
	m.Schedule() // 开启各种定时器
	return m, nil
}
