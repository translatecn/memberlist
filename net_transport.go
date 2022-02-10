package memberlist

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	sockAddr "github.com/hashicorp/go-sockaddr"
)

const (
	// udp读缓冲区大小
	udpPacketBufSize = 65536

	// udpRecvBufSize is a large buffer size that we attempt to set UDP
	// sockets to in order to handle a large volume of messages.
	udpRecvBufSize = 2 * 1024 * 1024
)

// NetTransportConfig 网络传输的配置
type NetTransportConfig struct {
	BindAddrs []string // [0.0.0.0]  ,假如本机有多块网卡
	BindPort  int      // 7946
	Logger    *log.Logger
}

// NetTransport 是一个传输实现，使用无连接的UDP进行数据包操作，并使用临时的TCP连接进行流操作。
type NetTransport struct {
	config       *NetTransportConfig
	packetCh     chan *Packet
	StreamCh     chan net.Conn
	Logger       *log.Logger
	Wg           sync.WaitGroup
	TcpListeners []*net.TCPListener
	UdpListeners []*net.UDPConn
	Shutdown     int32
}

var _ NodeAwareTransport = (*NetTransport)(nil)

// NewNetTransport 创建传输端点
func NewNetTransport(config *NetTransportConfig) (*NetTransport, error) {
	if len(config.BindAddrs) == 0 {
		return nil, fmt.Errorf("至少需要一个可以绑定的地址")
	}

	var ok bool
	t := NetTransport{
		config:   config,
		packetCh: make(chan *Packet), // 阻塞
		StreamCh: make(chan net.Conn),
		Logger:   config.Logger,
	}

	// 如果有错误、清理监听器
	defer func() {
		if !ok {
			t.SetShutdown()
		}
	}()

	// 构建TCP、UDP监听器
	port := config.BindPort
	for _, Addr := range config.BindAddrs {
		ip := net.ParseIP(Addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("启动TCP listener失败 %q Port %d: %v", Addr, port, err)
		}
		t.TcpListeners = append(t.TcpListeners, tcpLn)

		// 如果给定的配置端口为零，则使用第一个TCP监听器来挑选一个可用的端口，然后将其应用于其他所有的端口。
		if port == 0 {
			port = tcpLn.Addr().(*net.TCPAddr).Port
		}

		udpAddr := &net.UDPAddr{IP: ip, Port: port}
		udpLn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, fmt.Errorf("启动UDP listener失败 %q Port %d: %v", Addr, port, err)
		}
		if err := setUDPRecvBuf(udpLn); err != nil {
			return nil, fmt.Errorf("调整UDP缓冲区大小失败: %v", err)
		}
		t.UdpListeners = append(t.UdpListeners, udpLn)
	}

	// 现在我们已经能够创建它们了。
	for i := 0; i < len(config.BindAddrs); i++ {
		t.Wg.Add(2)
		// 开始接收请求
		go t.TcpListen(t.TcpListeners[i])
		go t.UdpListen(t.UdpListeners[i])
	}

	ok = true
	return &t, nil
}

// GetAutoBindPort 返回一个随机端口
func (t *NetTransport) GetAutoBindPort() int {
	return t.TcpListeners[0].Addr().(*net.TCPAddr).Port
}

// FinalAdvertiseAddr 返回广播地址.
func (t *NetTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {
		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("解析通信地址失败 %q", ip)
		}

		// 必要时确保IPv4转换。
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		// Config.go:177
		if t.config.BindAddrs[0] == "0.0.0.0" {
			// 否则，如果我们没有绑定到特定的IP，我们就使用合适的私有IP地址。
			var err error
			ip, err = sockAddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("获取通信地址失败: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("没有找到私有IP地址，也没有提供显式IP")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("无法解析广播地址: %q", ip)
			}
		} else {
			// 根据第一个TCP侦听器，使用我们绑定到的IP，我们已经确保它存在。
			advertiseAddr = t.TcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		// 使用绑定的端口
		advertisePort = t.GetAutoBindPort()
	}

	return advertiseAddr, advertisePort, nil
}

// WriteTo 发送数据到Addr
func (t *NetTransport) WriteTo(b []byte, Addr string) (time.Time, error) {
	a := Address{Addr: Addr, Name: ""}
	return t.WriteToAddress(b, a)
}

// WriteToAddress 往a发送数据
func (t *NetTransport) WriteToAddress(b []byte, a Address) (time.Time, error) {
	Addr := a.Addr
	udpAddr, err := net.ResolveUDPAddr("udp", Addr)
	if err != nil {
		return time.Time{}, err
	}
	_, err = t.UdpListeners[0].WriteTo(b, udpAddr)// 使用第一个网卡发送
	return time.Now(), err
}

// PacketCh 返回 packetCh
func (t *NetTransport) PacketCh() <-chan *Packet {
	return t.packetCh
}

// RecIngestPacket
func (t *NetTransport) RecIngestPacket(conn net.Conn, Addr net.Addr, now time.Time, shouldClose bool) error {
	if shouldClose {
		defer conn.Close()
	}

	// Copy everything from the stream into packet buffer.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, conn); err != nil {
		return fmt.Errorf("failed to read packet: %v", err)
	}

	// Check the length - it needs to have at least one byte to be a proper
	// message. This is checked elsewhere for writes coming in directly from
	// the UDP socket.
	if n := buf.Len(); n < 1 {
		return fmt.Errorf("packet too short (%d bytes) %s", n, pkg.LogAddress(Addr))
	}

	t.packetCh <- &Packet{
		Buf:       buf.Bytes(),
		From:      Addr,
		Timestamp: now,
	}
	return nil
}

// DialTimeout 与a建联，设置了超时时间
func (t *NetTransport) DialTimeout(Addr string, timeout time.Duration) (net.Conn, error) {
	a := Address{Addr: Addr, Name: ""}
	return t.DialAddressTimeout(a, timeout)
}

// DialAddressTimeout 与a建联，设置了超时时间
func (t *NetTransport) DialAddressTimeout(a Address, timeout time.Duration) (net.Conn, error) {
	Addr := a.Addr

	dialer := net.Dialer{Timeout: timeout}
	return dialer.Dial("tcp", Addr)
}

// GetStreamCh 返回新建立的流连接
func (t *NetTransport) GetStreamCh() <-chan net.Conn {
	//net.go:912
	return t.StreamCh
}

// See IngestionAwareTransport.
func (t *NetTransport) IngestStream(conn net.Conn) error {
	t.StreamCh <- conn
	return nil
}

// SetShutdown    .
func (t *NetTransport) SetShutdown() error {
	// 这将避免在我们关闭时出现关于错误的日志垃圾。
	atomic.StoreInt32(&t.Shutdown, 1)
	// 对所有的连接，关闭它们。
	for _, conn := range t.TcpListeners {
		conn.Close()
	}
	for _, conn := range t.UdpListeners {
		conn.Close()
	}
	t.Wg.Wait()
	return nil
}

// TcpListen 处理创建的TCP链接
func (t *NetTransport) TcpListen(tcpLn *net.TCPListener) {
	defer t.Wg.Done()

	//baseDelay是AcceptTCP()出错后，再次尝试的初始延迟。
	const baseDelay = 5 * time.Millisecond

	//maxDelay是AcceptTCP()出错后，再次尝试的最大延迟。在TcpListen()是错误循环的情况下，它将延迟关机检查。因此，对maxDelay的改变可能会对关机的延迟产生影响。
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		conn, err := tcpLn.AcceptTCP()
		if err != nil {
			if s := atomic.LoadInt32(&t.Shutdown); s == 1 {
				break
			}

			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			t.Logger.Printf("[错误] memberlist: 接收TCP链接失败: %v", err)
			time.Sleep(loopDelay)
			continue
		}
		// 没有错误，复位循环延迟
		loopDelay = 0
		//net.go:912
		t.StreamCh <- conn
	}
}

// UdpListen 处理创建的UDP链接,服务端
func (t *NetTransport) UdpListen(udpLn *net.UDPConn) {
	defer t.Wg.Done()
	for {
		buf := make([]byte, udpPacketBufSize)
		n, Addr, err := udpLn.ReadFrom(buf)
		ts := time.Now()
		if err != nil {
			// 出错时，判断是不是正在停止
			if s := atomic.LoadInt32(&t.Shutdown); s == 1 {
				break
			}

			t.Logger.Printf("[错误] memberlist:读取udp包失败: %v", err)
			continue
		}

		if n < 1 {
			t.Logger.Printf("[错误] memberlist:UDP包太小(%d bytes) %s", len(buf), pkg.LogAddress(Addr))
			continue
		}

		t.packetCh <- &Packet{
			Buf:       buf[:n], // udp包大小不会超过udpPacketBufSize
			From:      Addr,
			Timestamp: ts,
		}
	}
}

// setUDPRecvBuf is used to resize the UDP receive window. The function
// attempts to set the read buffer to `udpRecvBuf` but backs off until
// the read buffer can be set.
func setUDPRecvBuf(c *net.UDPConn) error {
	size := udpRecvBufSize
	var err error
	for size > 0 {
		if err = c.SetReadBuffer(size); err == nil {
			return nil
		}
		size = size / 2
	}
	return err
}
