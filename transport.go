package memberlist

import (
	"fmt"
	"net"
	"time"
)

// Packet is used to provide some metadata about incoming packets from peers
// over a packet connection, as well as the packet payload.
type Packet struct {
	// Buf has the raw contents of the packet.
	Buf []byte

	// From has the address of the peer. This is an actual net.Addr so we
	// can expose some concrete details about incoming packets.
	From net.Addr

	// Timestamp is the time when the packet was received. This should be
	// taken as close as possible to the actual receipt time to help make an
	// accurate RTT measurement during probes.
	Timestamp time.Time
}

// Transport is used to abstract over communicating with other peers. The packet
// interface is assumed to be best-effort and the stream interface is assumed to
// be reliable.
type Transport interface {
	// FinalAdvertiseAddr 返回所需的IP和端口，以通告到集群的其他部分。
	FinalAdvertiseAddr(ip string, port int) (net.IP, int, error)

	// WriteTo is a packet-oriented interface that fires off the given
	// payload to the given address in a connectionless fashion. This should
	// return a time stamp that's as close as possible to when the packet
	// was transmitted to help make accurate RTT measurements during probes.
	//
	// This is similar to net.PacketConn, though we didn't want to expose
	// that full set of required methods to keep assumptions about the
	// underlying plumbing to a minimum. We also treat the address here as a
	// string, similar to Dial, so it's network neutral, so this usually is
	// in the form of "host:port".
	WriteTo(b []byte, addr string) (time.Time, error)

	// PacketCh 直接消息传递; 读取来自其他节点的消息
	PacketCh() <-chan *Packet

	// DialTimeout is used to create a connection that allows us to perform
	// two-way communication with a peer. This is generally more expensive
	// than packet connections so is used for more infrequent operations
	// such as anti-entropy or fallback probes if the packet-oriented probe
	// failed.
	DialTimeout(addr string, timeout time.Duration) (net.Conn, error)

	// StreamCh pull模式、返回一个通道，可以处理来自其他节点传入流连接。
	StreamCh() <-chan net.Conn

	// Shutdown 停止、清理资源
	Shutdown() error
}

type Address struct {
	// Addr is a network address as a string, similar to Dial. This usually is
	// in the form of "host:port". This is required.
	Addr string

	// Name is the name of the node being addressed. This is optional but
	// transports may require it.
	Name string
}

func (a *Address) String() string {
	if a.Name != "" {
		return fmt.Sprintf("%s (%s)", a.Name, a.Addr)
	}
	return a.Addr
}

// IngestionAwareTransport is not used.
//
// Deprecated: IngestionAwareTransport is not used and may be removed in a future
// version. Define the interface locally instead of referencing this exported
// interface.
type IngestionAwareTransport interface {
	IngestPacket(conn net.Conn, addr net.Addr, now time.Time, shouldClose bool) error
	IngestStream(conn net.Conn) error
}

type NodeAwareTransport interface {
	Transport
	WriteToAddress(b []byte, addr Address) (time.Time, error)
	DialAddressTimeout(addr Address, timeout time.Duration) (net.Conn, error)
}

// shim 垫片
type shimNodeAwareTransport struct {
	Transport
}

var _ NodeAwareTransport = (*shimNodeAwareTransport)(nil)

func (t *shimNodeAwareTransport) WriteToAddress(b []byte, addr Address) (time.Time, error) {
	return t.WriteTo(b, addr.Addr)
}

func (t *shimNodeAwareTransport) DialAddressTimeout(addr Address, timeout time.Duration) (net.Conn, error) {
	return t.DialTimeout(addr.Addr, timeout)
}

type labelWrappedTransport struct {
	label string
	NodeAwareTransport
}

var _ NodeAwareTransport = (*labelWrappedTransport)(nil)

func (t *labelWrappedTransport) WriteToAddress(buf []byte, addr Address) (time.Time, error) {
	var err error
	buf, err = AddLabelHeaderToPacket(buf, t.label)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to add label header to packet: %w", err)
	}
	return t.NodeAwareTransport.WriteToAddress(buf, addr)
}

func (t *labelWrappedTransport) WriteTo(buf []byte, addr string) (time.Time, error) {
	var err error
	buf, err = AddLabelHeaderToPacket(buf, t.label)
	if err != nil {
		return time.Time{}, err
	}
	return t.NodeAwareTransport.WriteTo(buf, addr)
}

func (t *labelWrappedTransport) DialAddressTimeout(addr Address, timeout time.Duration) (net.Conn, error) {
	conn, err := t.NodeAwareTransport.DialAddressTimeout(addr, timeout)
	if err != nil {
		return nil, err
	}
	if err := AddLabelHeaderToStream(conn, t.label); err != nil {
		return nil, fmt.Errorf("failed to add label header to stream: %w", err)
	}
	return conn, nil
}

func (t *labelWrappedTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := t.NodeAwareTransport.DialTimeout(addr, timeout)
	if err != nil {
		return nil, err
	}
	if err := AddLabelHeaderToStream(conn, t.label); err != nil {
		return nil, fmt.Errorf("failed to add label header to stream: %w", err)
	}
	return conn, nil
}
