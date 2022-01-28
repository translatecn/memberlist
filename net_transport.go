package memberlist

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
	sockaddr "github.com/hashicorp/go-sockaddr"
)

const (
	// udpPacketBufSize is used to buffer incoming packets during read
	// operations.
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

// NetTransport is a Transport implementation that uses connectionless UDP for
// packet operations, and ad-hoc TCP connections for stream operations.
type NetTransport struct {
	config       *NetTransportConfig
	packetCh     chan *Packet
	streamCh     chan net.Conn
	logger       *log.Logger
	wg           sync.WaitGroup
	tcpListeners []*net.TCPListener
	udpListeners []*net.UDPConn
	shutdown     int32
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
		packetCh: make(chan *Packet),
		streamCh: make(chan net.Conn),
		logger:   config.Logger,
	}

	// 如果有错误、清理监听器
	defer func() {
		if !ok {
			t.Shutdown()
		}
	}()

	// 构建TCP、UDP监听器
	port := config.BindPort
	for _, addr := range config.BindAddrs {
		ip := net.ParseIP(addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("启动TCP listener失败 %q port %d: %v", addr, port, err)
		}
		t.tcpListeners = append(t.tcpListeners, tcpLn)

		// 如果给定的配置端口为零，则使用第一个TCP监听器来挑选一个可用的端口，然后将其应用于其他所有的端口。
		if port == 0 {
			port = tcpLn.Addr().(*net.TCPAddr).Port
		}

		udpAddr := &net.UDPAddr{IP: ip, Port: port}
		udpLn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, fmt.Errorf("启动UDP listener失败 %q port %d: %v", addr, port, err)
		}
		if err := setUDPRecvBuf(udpLn); err != nil {
			return nil, fmt.Errorf("调整UDP缓冲区大小失败: %v", err)
		}
		t.udpListeners = append(t.udpListeners, udpLn)
	}

	// 现在我们已经能够创建它们了。
	for i := 0; i < len(config.BindAddrs); i++ {
		t.wg.Add(2)
		// 开始接收请求
		go t.tcpListen(t.tcpListeners[i])
		go t.udpListen(t.udpListeners[i])
	}

	ok = true
	return &t, nil
}

// GetAutoBindPort 返回一个随机端口
func (t *NetTransport) GetAutoBindPort() int {
	return t.tcpListeners[0].Addr().(*net.TCPAddr).Port
}

// See Transport.
func (t *NetTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {
		// If they've supplied an address, use that.
		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("Failed to parse advertise address %q", ip)
		}

		// Ensure IPv4 conversion if necessary.
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		if t.config.BindAddrs[0] == "0.0.0.0" {
			// Otherwise, if we're not bound to a specific IP, let's
			// use a suitable private IP address.
			var err error
			ip, err = sockaddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("Failed to get interface addresses: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("No private IP address found, and explicit IP not provided")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("Failed to parse advertise address: %q", ip)
			}
		} else {
			// Use the IP that we're bound to, based on the first
			// TCP listener, which we already ensure is there.
			advertiseAddr = t.tcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		// Use the port we are bound to.
		advertisePort = t.GetAutoBindPort()
	}

	return advertiseAddr, advertisePort, nil
}

// See Transport.
func (t *NetTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	a := Address{Addr: addr, Name: ""}
	return t.WriteToAddress(b, a)
}

// See NodeAwareTransport.
func (t *NetTransport) WriteToAddress(b []byte, a Address) (time.Time, error) {
	addr := a.Addr

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return time.Time{}, err
	}

	// We made sure there's at least one UDP listener, so just use the
	// packet sending interface on the first one. Take the time after the
	// write call comes back, which will underestimate the time a little,
	// but help account for any delays before the write occurs.
	_, err = t.udpListeners[0].WriteTo(b, udpAddr)
	return time.Now(), err
}

// See Transport.
func (t *NetTransport) PacketCh() <-chan *Packet {
	return t.packetCh
}

// See IngestionAwareTransport.
func (t *NetTransport) IngestPacket(conn net.Conn, addr net.Addr, now time.Time, shouldClose bool) error {
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
		return fmt.Errorf("packet too short (%d bytes) %s", n, LogAddress(addr))
	}

	// Inject the packet.
	t.packetCh <- &Packet{
		Buf:       buf.Bytes(),
		From:      addr,
		Timestamp: now,
	}
	return nil
}

// See Transport.
func (t *NetTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	a := Address{Addr: addr, Name: ""}
	return t.DialAddressTimeout(a, timeout)
}

// See NodeAwareTransport.
func (t *NetTransport) DialAddressTimeout(a Address, timeout time.Duration) (net.Conn, error) {
	addr := a.Addr

	dialer := net.Dialer{Timeout: timeout}
	return dialer.Dial("tcp", addr)
}

// See Transport.
func (t *NetTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

// See IngestionAwareTransport.
func (t *NetTransport) IngestStream(conn net.Conn) error {
	t.streamCh <- conn
	return nil
}

// See Transport.
func (t *NetTransport) Shutdown() error {
	// This will avoid log spam about errors when we shut down.
	atomic.StoreInt32(&t.shutdown, 1)

	// Rip through all the connections and shut them down.
	for _, conn := range t.tcpListeners {
		conn.Close()
	}
	for _, conn := range t.udpListeners {
		conn.Close()
	}

	// Block until all the listener threads have died.
	t.wg.Wait()
	return nil
}

// tcpListen is a long running goroutine that accepts incoming TCP connections
// and hands them off to the stream channel.
func (t *NetTransport) tcpListen(tcpLn *net.TCPListener) {
	defer t.wg.Done()

	// baseDelay is the initial delay after an AcceptTCP() error before attempting again
	const baseDelay = 5 * time.Millisecond

	// maxDelay is the maximum delay after an AcceptTCP() error before attempting again.
	// In the case that tcpListen() is error-looping, it will delay the shutdown check.
	// Therefore, changes to maxDelay may have an effect on the latency of shutdown.
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		conn, err := tcpLn.AcceptTCP()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
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

			t.logger.Printf("[ERR] memberlist: Error accepting TCP connection: %v", err)
			time.Sleep(loopDelay)
			continue
		}
		// No error, reset loop delay
		loopDelay = 0

		t.streamCh <- conn
	}
}

// udpListen is a long running goroutine that accepts incoming UDP packets and
// hands them off to the packet channel.
func (t *NetTransport) udpListen(udpLn *net.UDPConn) {
	defer t.wg.Done()
	for {
		// Do a blocking read into a fresh buffer. Grab a time stamp as
		// close as possible to the I/O.
		buf := make([]byte, udpPacketBufSize)
		n, addr, err := udpLn.ReadFrom(buf)
		ts := time.Now()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
				break
			}

			t.logger.Printf("[ERR] memberlist: Error reading UDP packet: %v", err)
			continue
		}

		// Check the length - it needs to have at least one byte to be a
		// proper message.
		if n < 1 {
			t.logger.Printf("[ERR] memberlist: UDP packet too short (%d bytes) %s",
				len(buf), LogAddress(addr))
			continue
		}

		// Ingest the packet.
		metrics.IncrCounter([]string{"memberlist", "udp", "received"}, float32(n))
		t.packetCh <- &Packet{
			Buf:       buf[:n],
			From:      addr,
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
