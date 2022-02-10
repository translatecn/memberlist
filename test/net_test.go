package test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/memberlist"
	"io"
	"log"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/stretchr/testify/require"
)

// As a regression we left this test very low-level and network-ey, even after
// we abstracted the Transport. We added some basic network-free Transport tests
// in Transport_test.go to prove that we didn't hard code some network stuff
// outside of NetTransport.

func TestHandleCompoundPing(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := memberlist.Ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf, err := memberlist.Encode(memberlist.PingMsg, ping)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Make a compound message
	compound := memberlist.MakeCompoundMessage([][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()})

	// Send compound version
	Addr := &net.UDPAddr{IP: net.ParseIP(m.Config.BindAddr), Port: m.Config.BindPort}
	_, err = udp.WriteTo(compound.Bytes(), Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for responses
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	for i := 0; i < 3; i++ {
		in := make([]byte, 1500)
		n, _, err := udp.ReadFrom(in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		in = in[0:n]

		msgType := memberlist.MessageType(in[0])
		if msgType != memberlist.AckRespMsg {
			t.Fatalf("bad response %v", in)
		}

		var ack memberlist.AckResp
		if err := memberlist.Decode(in[1:], &ack); err != nil {
			t.Fatalf("unexpected err %s", err)
		}

		if ack.SeqNo != 42 {
			t.Fatalf("bad sequence no")
		}
	}

	doneCh <- struct{}{}
}

func TestHandlePing(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := memberlist.Ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf, err := memberlist.Encode(memberlist.PingMsg, ping)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Send
	Addr := &net.UDPAddr{IP: net.ParseIP(m.Config.BindAddr), Port: m.Config.BindPort}
	_, err = udp.WriteTo(buf.Bytes(), Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := memberlist.MessageType(in[0])
	if msgType != memberlist.AckRespMsg {
		t.Fatalf("bad response %v", in)
	}

	var ack memberlist.AckResp
	if err := memberlist.Decode(in[1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 42 {
		t.Fatalf("bad sequence no")
	}

	doneCh <- struct{}{}
}

func TestHandlePing_WrongNode(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping, wrong node!
	ping := memberlist.Ping{
		SeqNo:      42,
		Node:       m.Config.Name + "-bad",
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf, err := memberlist.Encode(memberlist.PingMsg, ping)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Send
	Addr := &net.UDPAddr{IP: net.ParseIP(m.Config.BindAddr), Port: m.Config.BindPort}
	_, err = udp.WriteTo(buf.Bytes(), Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	udp.SetDeadline(time.Now().Add(50 * time.Millisecond))
	in := make([]byte, 1500)
	_, _, err = udp.ReadFrom(in)

	// Should get an i/o timeout
	if err == nil {
		t.Fatalf("expected err %s", err)
	}
}

func TestHandleIndirectPing(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode an indirect ping
	ind := memberlist.IndirectPingReq{
		SeqNo:      100,
		Target:     net.ParseIP(m.Config.BindAddr),
		Port:       uint16(m.Config.BindPort),
		Node:       m.Config.Name,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf, err := memberlist.Encode(memberlist.IndirectPingMsg, &ind)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Send
	Addr := &net.UDPAddr{IP: net.ParseIP(m.Config.BindAddr), Port: m.Config.BindPort}
	_, err = udp.WriteTo(buf.Bytes(), Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := memberlist.MessageType(in[0])
	if msgType != memberlist.AckRespMsg {
		t.Fatalf("bad response %v", in)
	}

	var ack memberlist.AckResp
	if err := memberlist.Decode(in[1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 100 {
		t.Fatalf("bad sequence no")
	}

	doneCh <- struct{}{}
}

func TestTCPPing(t *testing.T) {
	var tcp *net.TCPListener
	var tcpAddr *net.TCPAddr
	for port := 60000; port < 61000; port++ {
		tcpAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err == nil {
			tcp = tcpLn
			break
		}
	}
	if tcp == nil {
		t.Fatalf("no tcp listener")
	}

	tcpAddr2 := memberlist.Address{Addr: tcpAddr.String(), Name: "test"}

	// Note that tcp gets closed in the last test, so we avoid a deferred
	// Close() call here.

	m := GetMemberlist(t, nil)
	defer m.SetShutdown()

	pingTimeout := m.Config.ProbeInterval
	pingTimeMax := m.Config.ProbeInterval + 10*time.Millisecond

	// Do a normal round trip.
	pingOut := memberlist.Ping{SeqNo: 23, Node: "mongo"}
	pingErrCh := make(chan error, 1)
	go func() {
		tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer conn.Close()

		msgType, _, dec, err := m.ReadStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		if msgType != memberlist.PingMsg {
			pingErrCh <- fmt.Errorf("expecting ping, got message type (%d)", msgType)
			return
		}

		var pingIn memberlist.Ping
		if err := dec.Decode(&pingIn); err != nil {
			pingErrCh <- fmt.Errorf("failed to decode ping: %s", err)
			return
		}

		if pingIn.SeqNo != pingOut.SeqNo {
			pingErrCh <- fmt.Errorf("sequence number isn't correct (%d) vs (%d)", pingIn.SeqNo, pingOut.SeqNo)
			return
		}

		if pingIn.Node != pingOut.Node {
			pingErrCh <- fmt.Errorf("node name isn't correct (%s) vs (%s)", pingIn.Node, pingOut.Node)
			return
		}

		ack := memberlist.AckResp{SeqNo: pingIn.SeqNo}
		out, err := memberlist.Encode(memberlist.AckRespMsg, &ack)
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to encode ack: %s", err)
			return
		}

		err = m.RawSendMsgStream(conn, out.Bytes(), "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send ack: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	Deadline := time.Now().Add(pingTimeout)
	didContact, err := m.SendPingAndWaitForAck(tcpAddr2, pingOut, Deadline)
	if err != nil {
		t.Fatalf("error trying to ping: %s", err)
	}
	if !didContact {
		t.Fatalf("expected successful ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure a mis-matched sequence number is caught.
	go func() {
		tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer conn.Close()

		_, _, dec, err := m.ReadStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		var pingIn memberlist.Ping
		if err := dec.Decode(&pingIn); err != nil {
			pingErrCh <- fmt.Errorf("failed to decode ping: %s", err)
			return
		}

		ack := memberlist.AckResp{SeqNo: pingIn.SeqNo + 1}
		out, err := memberlist.Encode(memberlist.AckRespMsg, &ack)
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to encode ack: %s", err)
			return
		}

		err = m.RawSendMsgStream(conn, out.Bytes(), "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send ack: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	Deadline = time.Now().Add(pingTimeout)
	didContact, err = m.SendPingAndWaitForAck(tcpAddr2, pingOut, Deadline)
	if err == nil || !strings.Contains(err.Error(), "Sequence number") {
		t.Fatalf("expected an error from mis-matched sequence number")
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure an unexpected message type is handled gracefully.
	go func() {
		tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer conn.Close()

		_, _, _, err = m.ReadStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		bogus := memberlist.IndirectPingReq{}
		out, err := memberlist.Encode(memberlist.IndirectPingMsg, &bogus)
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to encode bogus msg: %s", err)
			return
		}

		err = m.RawSendMsgStream(conn, out.Bytes(), "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send bogus msg: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	Deadline = time.Now().Add(pingTimeout)
	didContact, err = m.SendPingAndWaitForAck(tcpAddr2, pingOut, Deadline)
	if err == nil || !strings.Contains(err.Error(), "Unexpected msgType") {
		t.Fatalf("expected an error from bogus message")
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure failed I/O respects the Deadline. In this case we try the
	// common case of the receiving node being totally down.
	tcp.Close()
	Deadline = time.Now().Add(pingTimeout)
	startPing := time.Now()
	didContact, err = m.SendPingAndWaitForAck(tcpAddr2, pingOut, Deadline)
	pingTime := time.Now().Sub(startPing)
	if err != nil {
		t.Fatalf("expected no error during ping on closed socket, got: %s", err)
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if pingTime > pingTimeMax {
		t.Fatalf("took too long to fail ping, %9.6f", pingTime.Seconds())
	}
}

func TestTCPPushPull(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.SetShutdown()

	m.Nodes = append(m.Nodes, &memberlist.NodeState{
		Node: memberlist.Node{
			Name: "Test 0",
			Addr: net.ParseIP(m.Config.BindAddr),
			Port: uint16(m.Config.BindPort),
		},
		Incarnation: 0,
		State:       memberlist.StateSuspect,
		StateChange: time.Now().Add(-1 * time.Second),
	})

	Addr := net.JoinHostPort(m.Config.BindAddr, strconv.Itoa(m.Config.BindPort))
	conn, err := net.Dial("tcp", Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer conn.Close()

	localNodes := make([]memberlist.PushNodeState, 3)
	localNodes[0].Name = "Test 0"
	localNodes[0].Addr = net.ParseIP(m.Config.BindAddr)
	localNodes[0].Port = uint16(m.Config.BindPort)
	localNodes[0].Incarnation = 1
	localNodes[0].State = memberlist.StateAlive
	localNodes[1].Name = "Test 1"
	localNodes[1].Addr = net.ParseIP(m.Config.BindAddr)
	localNodes[1].Port = uint16(m.Config.BindPort)
	localNodes[1].Incarnation = 1
	localNodes[1].State = memberlist.StateAlive
	localNodes[2].Name = "Test 2"
	localNodes[2].Addr = net.ParseIP(m.Config.BindAddr)
	localNodes[2].Port = uint16(m.Config.BindPort)
	localNodes[2].Incarnation = 1
	localNodes[2].State = memberlist.StateAlive

	// Send our node state
	header := memberlist.PushPullHeader{Nodes: 3}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(conn, &hd)

	// Send the push/pull indicator
	conn.Write([]byte{byte(memberlist.PushPullMsg)})

	if err := enc.Encode(&header); err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	for i := 0; i < header.Nodes; i++ {
		if err := enc.Encode(&localNodes[i]); err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	// Read the message type
	var msgType memberlist.MessageType
	if err := binary.Read(conn, binary.BigEndian, &msgType); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	var bufConn io.Reader = conn
	msghd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(bufConn, &msghd)

	// Check if we have a Compressed message
	if msgType == memberlist.CompressMsg {
		var c memberlist.Compress
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		decomp, err := memberlist.DeCompressBuffer(&c)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}

		// Reset the message type
		msgType = memberlist.MessageType(decomp[0])

		// Create a new bufConn
		bufConn = bytes.NewReader(decomp[1:])

		// Create a new decoder
		dec = codec.NewDecoder(bufConn, &hd)
	}

	// Quit if not push/pull
	if msgType != memberlist.PushPullMsg {
		t.Fatalf("bad message type")
	}

	if err := dec.Decode(&header); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Allocate space for the transfer
	remoteNodes := make([]memberlist.PushNodeState, header.Nodes)

	// Try to decode all the states
	for i := 0; i < header.Nodes; i++ {
		if err := dec.Decode(&remoteNodes[i]); err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	if len(remoteNodes) != 1 {
		t.Fatalf("bad response")
	}

	n := &remoteNodes[0]
	if n.Name != "Test 0" {
		t.Fatalf("bad name")
	}
	if bytes.Compare(n.Addr, net.ParseIP(m.Config.BindAddr)) != 0 {
		t.Fatal("bad Addr")
	}
	if n.Incarnation != 0 {
		t.Fatal("bad incarnation")
	}
	if n.State != memberlist.StateSuspect {
		t.Fatal("bad state")
	}
}

func TestSendMsg_Piggyback(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.SetShutdown()

	// Add a message to be broadcast
	a := memberlist.Alive{
		Incarnation: 10,
		Node:        "rand",
		Addr:        []byte{127, 0, 0, 255},
		Meta:        nil,
		Vsn: []uint8{
			memberlist.ProtocolVersionMin, memberlist.ProtocolVersionMax, memberlist.ProtocolVersionMin,
			1, 1, 1,
		},
	}
	m.EncodeBroadcast("rand", memberlist.AliveMsg, &a)

	udp := listenUDP(t)
	defer udp.Close()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := memberlist.Ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf, err := memberlist.Encode(memberlist.PingMsg, ping)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Send
	Addr := &net.UDPAddr{IP: net.ParseIP(m.Config.BindAddr), Port: m.Config.BindPort}
	_, err = udp.WriteTo(buf.Bytes(), Addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := memberlist.MessageType(in[0])
	if msgType != memberlist.CompoundMsg {
		t.Fatalf("bad response %v", in)
	}

	// get the parts
	trunc, parts, err := memberlist.DecodeCompoundMessage(in[1:])
	if trunc != 0 {
		t.Fatalf("意外截断")
	}
	if len(parts) != 2 {
		t.Fatalf("unexpected parts %v", parts)
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	var ack memberlist.AckResp
	if err := memberlist.Decode(parts[0][1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 42 {
		t.Fatalf("bad sequence no")
	}

	var Aliveout memberlist.Alive
	if err := memberlist.Decode(parts[1][1:], &Aliveout); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if Aliveout.Node != "rand" || Aliveout.Incarnation != 10 {
		t.Fatalf("bad mesg")
	}

	doneCh <- struct{}{}
}

func TestEncryptDecryptState(t *testing.T) {
	state := []byte("this is our internal state...")
	config := &memberlist.Config{
		SecretKey:       []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		ProtocolVersion: memberlist.ProtocolVersionMax,
	}

	m, err := memberlist.Create(config)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer m.SetShutdown()

	crypt, err := m.EncryptLocalState(state, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Create reader, seek past the type byte
	buf := bytes.NewReader(crypt)
	buf.Seek(1, 0)

	plain, err := m.DecryptRemoteState(buf, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(state, plain) {
		t.Fatalf("Decrypt failed: %v", plain)
	}
}

func TestRawSendUdp_CRC(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	a := memberlist.Address{
		Addr: udp.LocalAddr().String(),
		Name: "test",
	}

	// Pass a nil node with no Nodes registered, should result in no checksum
	payload := []byte{3, 3, 3, 3}
	m.RawSendMsgPacket(a, nil, payload)

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 4 {
		t.Fatalf("bad: %v", in)
	}

	// Pass a non-nil node with PMax >= 5, should result in a checksum
	m.RawSendMsgPacket(a, &memberlist.Node{PMax: 5}, payload)

	in = make([]byte, 1500)
	n, _, err = udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}

	// Register a node with PMax >= 5 to be looked up, should result in a checksum
	m.NodeMap["127.0.0.1"] = &memberlist.NodeState{
		Node: memberlist.Node{PMax: 5},
	}
	m.RawSendMsgPacket(a, nil, payload)

	in = make([]byte, 1500)
	n, _, err = udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}
}

func TestIngestPacket_CRC(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	a := memberlist.Address{
		Addr: udp.LocalAddr().String(),
		Name: "test",
	}

	// Get a message with a checksum
	payload := []byte{3, 3, 3, 3}
	m.RawSendMsgPacket(a, &memberlist.Node{PMax: 5}, payload)

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}

	// Corrupt the checksum
	in[1] <<= 1

	logs := &bytes.Buffer{}
	Logger := log.New(logs, "", 0)
	m.Logger = Logger
	m.HandleIngestPacket(in, udp.LocalAddr(), time.Now())

	if !strings.Contains(logs.String(), "invalid checksum") {
		t.Fatalf("bad: %s", logs.String())
	}
}

func TestIngestPacket_ExportedFunc_EmptyMessage(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.EnableCompression = false
	})
	defer m.SetShutdown()

	udp := listenUDP(t)
	defer udp.Close()

	emptyConn := &emptyReadNetConn{}

	logs := &bytes.Buffer{}
	Logger := log.New(logs, "", 0)
	m.Logger = Logger

	type ingestionAwareTransport interface {
		RecIngestPacket(conn net.Conn, Addr net.Addr, now time.Time, shouldClose bool) error
	}

	err := m.Transport.(ingestionAwareTransport).RecIngestPacket(emptyConn, udp.LocalAddr(), time.Now(), true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "packet too short")
}

type emptyReadNetConn struct {
	net.Conn
}

func (c *emptyReadNetConn) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (c *emptyReadNetConn) Close() error {
	return nil
}

func TestGossip_MismatchedKeys(t *testing.T) {
	// Create two agents with different gossip keys
	c1 := testConfig(t)
	c1.SecretKey = []byte("4W6DGn2VQVqDEceOdmuRTQ==")

	m1, err := memberlist.Create(c1)
	require.NoError(t, err)
	defer m1.SetShutdown()

	bindPort := m1.Config.BindPort

	c2 := testConfig(t)
	c2.BindPort = bindPort
	c2.SecretKey = []byte("XhX/w702/JKKK7/7OtM9Ww==")

	m2, err := memberlist.Create(c2)
	require.NoError(t, err)
	defer m2.SetShutdown()

	// Make sure we get this error on the joining side
	_, err = m2.Join([]string{c1.Name + "/" + c1.BindAddr})
	if err == nil || !strings.Contains(err.Error(), "No installed keys could decrypt the message") {
		t.Fatalf("bad: %s", err)
	}
}

func listenUDP(t *testing.T) *net.UDPConn {
	var udp *net.UDPConn
	for port := 60000; port < 61000; port++ {
		udpAddr := fmt.Sprintf("127.0.0.1:%d", port)
		udpLn, err := net.ListenPacket("udp", udpAddr)
		if err == nil {
			udp = udpLn.(*net.UDPConn)
			break
		}
	}
	if udp == nil {
		t.Fatalf("no udp listener")
	}
	return udp
}

func TestHandleCommand(t *testing.T) {
	var buf bytes.Buffer
	m := memberlist.Members{
		Logger: log.New(&buf, "", 0),
	}
	m.HandleCommand(nil, &net.TCPAddr{Port: 12345}, time.Now())
	require.Contains(t, buf.String(), "missing message type byte")
}
