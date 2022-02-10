package memberlist

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"hash/crc32"
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

const (
	ProtocolVersionMin         uint8 = 1 // 协议版本
	ProtocolVersion2Compatible       = 2
	ProtocolVersionMax               = 5
)

// MessageType 一个字节的大小，消息类型
type MessageType uint8

const (
	PingMsg MessageType = iota
	IndirectPingMsg
	AckRespMsg
	SuspectMsg // 怀疑消息
	AliveMsg   // 探活消息
	DeadMsg    // 死亡消息
	PushPullMsg
	CompoundMsg
	UserMsg // 用户消息、不处理
	CompressMsg
	EncryptMsg
	NAckRespMsg
	HasCrcMsg
	ErrMsg
)

const (
	// HasLabelMsg有一个特意的高值，这样你就可以从EncryptionVersion标头（现在是0/1）
	// 和任何现有的MessageTypes中辨别出来。
	HasLabelMsg MessageType = 244
)

// CompressionType is used to specify the Compression algorithm
type CompressionType uint8

const (
	lzwAlgo CompressionType = iota
)

const (
	MetaMaxSize            = 512 // 节点元数据最大的大小
	CompoundHeaderOverhead = 2   // 假定header开销
	CompoundOverhead       = 2   // 假定在compoundHeader中每个条目的开销
	UserMsgOverhead        = 1
	blockingWarning        = 10 * time.Millisecond // 如果UDP数据包需要这么长的时间来处理，则发出警告
	maxPushStateBytes      = 20 * 1024 * 1024
	maxPushPullRequests    = 128 // 最大并发推/拉请求数
)

type peekedConn struct {
	// 已经从链接中读取的数据,但是还没有被read,一旦read 就没有了
	Peeked []byte
	net.Conn
}

func (c *peekedConn) Read(p []byte) (n int, err error) {
	if len(c.Peeked) > 0 {
		n = copy(p, c.Peeked)
		c.Peeked = c.Peeked[n:]
		if len(c.Peeked) == 0 {
			c.Peeked = nil
		}
		return n, nil
	}
	return c.Conn.Read(p)
}

// Ping request sent directly to node
type Ping struct {
	SeqNo uint32

	// Node is sent so the target can verify they are
	// the intended recipient. This is to protect again an agent
	// restart with a new name.
	Node string

	SourceAddr []byte `codec:",omitempty"` // Source Address, used for a direct reply
	SourcePort uint16 `codec:",omitempty"` // Source Port, used for a direct reply
	SourceNode string `codec:",omitempty"` // Source name, used for a direct reply
}

// indirect Ping sent to an indirect node
type IndirectPingReq struct {
	SeqNo  uint32
	Target []byte
	Port   uint16

	// Node is sent so the target can verify they are
	// the intended recipient. This is to protect against an agent
	// restart with a new name.
	Node string

	Nack bool // true if we'd like a nack back

	SourceAddr []byte `codec:",omitempty"` // Source Address, used for a direct reply
	SourcePort uint16 `codec:",omitempty"` // Source Port, used for a direct reply
	SourceNode string `codec:",omitempty"` // Source name, used for a direct reply
}

// AckResp ack response is sent for a Ping
type AckResp struct {
	SeqNo   uint32
	Payload []byte
}

// nack response is sent for an indirect Ping when the pinger doesn't hear from
// the Ping-ee within the configured timeout. This lets the original node know
// that the indirect Ping attempt happened but didn't succeed.
type NAckResp struct {
	SeqNo uint32
}

// err response is sent to relay the error from the remote end
type errResp struct {
	Error string
}

// Suspect is broadcast when we Suspect a node is Dead
type Suspect struct {
	Incarnation uint32
	Node        string
	From        string // Include who is Suspecting
}

// Alive 是在我们知道一个节点是活的时候 广播。 对加入的节点进行重载
type Alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
	Port        uint16
	Meta        []byte

	// protocol/delegate各个版本、按照如下排序
	// pmin, pmax, pcur, dmin, dmax, dcur
	Vsn []uint8
}

// Dead is broadcast when we confirm a node is Dead
// Overloaded for Nodes leaving
type Dead struct {
	Incarnation uint32
	Node        string
	From        string // Include who is Suspecting
}

// PushPullHeader 用来通知对方我们要转移多少个state
type PushPullHeader struct {
	Nodes        int  // 节点数量
	UserStateLen int  // 节点状态数据长度
	Join         bool // 是否加入集群
}

// UserMsgHeader is used to encapsulate a UserMsg
type UserMsgHeader struct {
	UserMsgLen int // Encodes the byte lengh of user state
}

// PushNodeState PushPullReq 传输本地的节点状态
type PushNodeState struct {
	Name        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Incarnation uint32
	State       NodeStateType
	Vsn         []uint8 // 协议版本
}

// Compress is used to wrap an underlying payload
// using a specified Compression algorithm
type Compress struct {
	Algo CompressionType
	Buf  []byte
}

// msgHandoff 用于在goroutine之间消息传递
type msgHandoff struct {
	msgType MessageType
	buf     []byte
	from    net.Addr
}

// EncryptionVersion 返回加密版本
func (m *Members) EncryptionVersion() EncryptionVersion {
	switch m.ProtocolVersion() {
	case 1:
		return 0
	default:
		return 1
	}
}

// streamListen pull、push 模式, 处理来自其他节点的数据
func (m *Members) streamListen() {
	for {
		select {
		case conn := <-m.Transport.StreamCh():
			go m.handleConn(conn)
		case <-m.shutdownCh:
			return
		}
	}
}

// handleConn 处理pull、push模式下的流链接
func (m *Members) handleConn(conn net.Conn) {
	defer conn.Close()
	m.Logger.Printf("[DEBUG] memberlist: 流连接 %s", LogConn(conn))

	conn.SetDeadline(time.Now().Add(m.Config.TCPTimeout))

	var (
		streamLabel string
		err         error
	)
	conn, streamLabel, err = RemoveLabelHeaderFromStream(conn)
	if err != nil {
		m.Logger.Printf("[错误] memberlist: 未能接收和删除流标签头: %s %s", err, LogConn(conn))
		return
	}

	if m.Config.SkipInboundLabelCheck {
		if streamLabel != "" {
			m.Logger.Printf("[错误] memberlist: 意外的流标签头: %s", LogConn(conn))
			return
		}
		streamLabel = m.Config.Label
	}

	if m.Config.Label != streamLabel {
		m.Logger.Printf("[错误] memberlist: 丢弃带有不可接受的标签的流 %q: %s", streamLabel, LogConn(conn))
		return
	}

	msgType, bufConn, dec, err := m.ReadStream(conn, streamLabel)
	if err != nil {
		if err != io.EOF {
			m.Logger.Printf("[错误] memberlist: 接受失败: %s %s", err, LogConn(conn))

			resp := errResp{err.Error()}
			out, err := Encode(ErrMsg, &resp)
			if err != nil {
				m.Logger.Printf("[错误] memberlist: 响应编码失败: %s", err)
				return
			}

			err = m.RawSendMsgStream(conn, out.Bytes(), streamLabel)
			if err != nil {
				m.Logger.Printf("[错误] memberlist: 发送失败: %s %s", err, LogConn(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case UserMsg:
		if err := m.readUserMsg(bufConn, dec); err != nil {
			m.Logger.Printf("[错误] memberlist: 接收用户消息失败: %s %s", err, LogConn(conn))
		}
	case PushPullMsg:
		// 增加计数器 pending push/pulls
		numConcurrent := atomic.AddUint32(&m.PushPullReq, 1)
		defer atomic.AddUint32(&m.PushPullReq, ^uint32(0)) // 减1

		if numConcurrent >= maxPushPullRequests {
			m.Logger.Printf("[错误] memberlist: 太多 pending push/pull requests")
			return
		}

		join, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: 读取远端state失败: %s %s", err, LogConn(conn))
			return
		}

		if err := m.sendLocalState(conn, join, streamLabel); err != nil {
			m.Logger.Printf("[错误] memberlist:发送本地state失败: %s %s", err, LogConn(conn))
			return
		}

		if err := m.mergeRemoteState(join, remoteNodes, userState); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed push/pull merge: %s %s", err, LogConn(conn))
			return
		}
	case PingMsg:
		var p Ping
		if err := dec.Decode(&p); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to Decode Ping: %s %s", err, LogConn(conn))
			return
		}

		if p.Node != "" && p.Node != m.Config.Name {
			m.Logger.Printf("[WARN] memberlist: Got Ping for unexpected node %s %s", p.Node, LogConn(conn))
			return
		}

		ack := AckResp{p.SeqNo, nil}
		out, err := Encode(AckRespMsg, &ack)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to Encode ack: %s", err)
			return
		}

		err = m.RawSendMsgStream(conn, out.Bytes(), streamLabel)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to send ack: %s %s", err, LogConn(conn))
			return
		}
	default:
		m.Logger.Printf("[错误] memberlist: 收到了无效的消息类型 (%d) %s", msgType, LogConn(conn))
	}
}

// packetListen 将包从传输中取出，并处理
func (m *Members) packetListen() {
	for {
		select {
		case packet := <-m.Transport.PacketCh(): // 直接消息传递; 读取来自其他节点的消息
			m.IngestPacket(packet.Buf, packet.From, packet.Timestamp)

		case <-m.shutdownCh:
			return
		}
	}
}

// 接收包
func (m *Members) IngestPacket(buf []byte, from net.Addr, timestamp time.Time) {
	var (
		packetLabel string
		err         error
	)
	buf, packetLabel, err = RemoveLabelHeaderFromPacket(buf) // 移除标签头
	if err != nil {
		m.Logger.Printf("[错误] memberlist: %v %s", err, LogAddress(from))
		return
	}

	if m.Config.SkipInboundLabelCheck {
		if packetLabel != "" {
			m.Logger.Printf("[错误] memberlist: unexpected double packet label header: %s", LogAddress(from))
			return
		}
		// Set this from Config so that the auth data assertions work below.
		packetLabel = m.Config.Label
	}

	if m.Config.Label != packetLabel {
		m.Logger.Printf("[错误] memberlist: discarding packet with unacceptable label %q: %s", packetLabel, LogAddress(from))
		return
	}

	// Check if encryption is enabled
	if m.Config.EncryptionEnabled() {
		// Decrypt the payload
		authData := []byte(packetLabel)
		plain, err := DecryptPayload(m.Config.Keyring.GetKeys(), buf, authData)
		if err != nil {
			if !m.Config.GossipVerifyIncoming {
				// Treat the message as plaintext
				plain = buf
			} else {
				m.Logger.Printf("[错误] memberlist: Decrypt packet failed: %v %s", err, LogAddress(from))
				return
			}
		}

		// Continue processing the plaintext buffer
		buf = plain
	}

	// See if there's a checksum included to verify the contents of the message
	if len(buf) >= 5 && MessageType(buf[0]) == HasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			m.Logger.Printf("[WARN] memberlist: Got invalid checksum for UDP packet: %x, %x", crc, expected)
			return
		}
		m.HandleCommand(buf[5:], from, timestamp)
	} else {
		m.HandleCommand(buf, from, timestamp)
	}
}

func (m *Members) HandleCommand(buf []byte, from net.Addr, timestamp time.Time) {
	if len(buf) < 1 {
		m.Logger.Printf("[错误] memberlist: missing message type byte %s", LogAddress(from))
		return
	}
	// Decode the message type
	msgType := MessageType(buf[0])
	buf = buf[1:]

	// Switch on the msgType
	switch msgType {
	case CompoundMsg:
		m.handleCompound(buf, from, timestamp)
	case CompressMsg:
		m.handleCompressed(buf, from, timestamp)

	case PingMsg:
		m.handlePing(buf, from)
	case IndirectPingMsg:
		m.handleIndirectPing(buf, from)
	case AckRespMsg:
		m.handleAck(buf, from, timestamp)
	case NAckRespMsg:
		m.handleNack(buf, from)

	case SuspectMsg:
		fallthrough
	case AliveMsg:
		fallthrough
	case DeadMsg:
		fallthrough
	case UserMsg:
		// Determine the message queue_broadcast, prioritize Alive
		queue := m.lowPriorityMsgQueue
		if msgType == AliveMsg {
			queue = m.highPriorityMsgQueue
		}

		// Check for overflow and append if not full
		m.msgQueueLock.Lock()
		if queue.Len() >= m.Config.HandoffQueueDepth {
			m.Logger.Printf("[WARN] memberlist: handler queue_broadcast full, dropping message (%d) %s", msgType, LogAddress(from))
		} else {
			queue.PushBack(msgHandoff{msgType, buf, from})
		}
		m.msgQueueLock.Unlock()

		// Notify of pending message
		select {
		case m.handoffCh <- struct{}{}:
		default:
		}

	default:
		m.Logger.Printf("[错误] memberlist: msg type (%d) not supported %s", msgType, LogAddress(from))
	}
}

// getNextMessage 使用LIFO，按优先级顺序返回下一个要处理的消息
func (m *Members) getNextMessage() (msgHandoff, bool) {
	m.msgQueueLock.Lock()
	defer m.msgQueueLock.Unlock()

	if el := m.highPriorityMsgQueue.Back(); el != nil {
		m.highPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	} else if el := m.lowPriorityMsgQueue.Back(); el != nil {
		m.lowPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	}
	return msgHandoff{}, false
}

// packetHandler 从listener解耦出来,避免阻塞 ,导致ping\ack延迟
func (m *Members) packetHandler() {
	for {
		select {
		case <-m.handoffCh: // 用户消息ch,当收到UserMsg时激活
			for {
				msg, ok := m.getNextMessage() // 从队列里拿,
				if !ok {
					break
				}
				msgType := msg.msgType
				buf := msg.buf
				from := msg.from

				switch msgType {
				case SuspectMsg:
					m.handleSuspect(buf, from)
				case AliveMsg:
					m.handleAlive(buf, from)
				case DeadMsg:
					m.handleDead(buf, from)
				case UserMsg:
					m.handleUser(buf, from)
				default:
					m.Logger.Printf("[错误] memberlist: 消息类型不支持 (%d) 不支持 %s (packet handler)", msgType, LogAddress(from))
				}
			}

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Members) handleCompound(buf []byte, from net.Addr, timestamp time.Time) {
	// Decode the parts
	trunc, parts, err := DecodeCompoundMessage(buf)
	if err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode compound request: %s %s", err, LogAddress(from))
		return
	}

	// Log any truncation
	if trunc > 0 {
		m.Logger.Printf("[WARN] memberlist: Compound request had %d truncated messages %s", trunc, LogAddress(from))
	}

	// Handle each message
	for _, part := range parts {
		m.HandleCommand(part, from, timestamp)
	}
}

func (m *Members) handlePing(buf []byte, from net.Addr) {
	var p Ping
	if err := Decode(buf, &p); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode Ping request: %s %s", err, LogAddress(from))
		return
	}
	// If node is provided, verify that it is for us
	if p.Node != "" && p.Node != m.Config.Name {
		m.Logger.Printf("[WARN] memberlist: Got Ping for unexpected node '%s' %s", p.Node, LogAddress(from))
		return
	}
	var ack AckResp
	ack.SeqNo = p.SeqNo
	if m.Config.Ping != nil {
		ack.Payload = m.Config.Ping.AckPayload()
	}

	Addr := ""
	if len(p.SourceAddr) > 0 && p.SourcePort > 0 {
		Addr = pkg.JoinHostPort(net.IP(p.SourceAddr).String(), p.SourcePort)
	} else {
		Addr = from.String()
	}

	a := Address{
		Addr: Addr,
		Name: p.SourceNode,
	}
	if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to send ack: %s %s", err, LogAddress(from))
	}
}

func (m *Members) handleIndirectPing(buf []byte, from net.Addr) {
	var ind IndirectPingReq
	if err := Decode(buf, &ind); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode indirect Ping request: %s %s", err, LogAddress(from))
		return
	}

	// For proto versions < 2, there is no Port provided. Mask old
	// behavior by using the configured Port.
	if m.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(m.Config.BindPort)
	}

	// Send a Ping to the correct host.
	localSeqNo := m.NextSeqNo()
	selfAddr, selfPort := m.getAdvertise()
	ping := Ping{
		SeqNo: localSeqNo,
		Node:  ind.Node,
		// The outbound message is Addressed FROM us.
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.Config.Name,
	}

	// Forward the ack back to the requestor. If the request encodes an origin
	// use that otherwise assume that the other end of the UDP socket is
	// usable.
	indAddr := ""
	if len(ind.SourceAddr) > 0 && ind.SourcePort > 0 {
		indAddr = pkg.JoinHostPort(net.IP(ind.SourceAddr).String(), ind.SourcePort)
	} else {
		indAddr = from.String()
	}

	// Setup a response handler to relay the ack
	cancelCh := make(chan struct{})
	respHandler := func(payload []byte, timestamp time.Time) {
		// Try to prevent the nack if we've caught it in time.
		close(cancelCh)

		ack := AckResp{ind.SeqNo, nil}
		a := Address{
			Addr: indAddr,
			Name: ind.SourceNode,
		}
		if err := m.encodeAndSendMsg(a, AckRespMsg, &ack); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to forward ack: %s %s", err, LogStringAddress(indAddr))
		}
	}
	m.SetAckHandler(localSeqNo, respHandler, m.Config.ProbeTimeout)

	// Send the Ping.
	Addr := pkg.JoinHostPort(net.IP(ind.Target).String(), ind.Port)
	a := Address{
		Addr: Addr,
		Name: ind.Node,
	}
	if err := m.encodeAndSendMsg(a, PingMsg, &ping); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to send indirect Ping: %s %s", err, LogStringAddress(indAddr))
	}

	// Setup a timer to fire off a nack if no ack is seen in time.
	if ind.Nack {
		go func() {
			select {
			case <-cancelCh:
				return
			case <-time.After(m.Config.ProbeTimeout):
				nack := NAckResp{ind.SeqNo}
				a := Address{
					Addr: indAddr,
					Name: ind.SourceNode,
				}
				if err := m.encodeAndSendMsg(a, NAckRespMsg, &nack); err != nil {
					m.Logger.Printf("[错误] memberlist: Failed to send nack: %s %s", err, LogStringAddress(indAddr))
				}
			}
		}()
	}
}

func (m *Members) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack AckResp
	if err := Decode(buf, &ack); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode ack response: %s %s", err, LogAddress(from))
		return
	}
	m.InvokeAckHandler(ack, timestamp)
}

func (m *Members) handleNack(buf []byte, from net.Addr) {
	var nack NAckResp
	if err := Decode(buf, &nack); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode nack response: %s %s", err, LogAddress(from))
		return
	}
	m.InvokeNAckHandler(nack)
}

func (m *Members) handleSuspect(buf []byte, from net.Addr) {
	var sus Suspect
	if err := Decode(buf, &sus); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode Suspect message: %s %s", err, LogAddress(from))
		return
	}
	m.SuspectNode(&sus)
}

// ensureCanConnect return the IP from a RemoteAddress
// return error if this client must not connect
func (m *Members) ensureCanConnect(from net.Addr) error {
	if !m.Config.IPMustBeChecked() {
		return nil
	}
	source := from.String()
	if source == "pipe" {
		return nil
	}
	host, _, err := net.SplitHostPort(source)
	if err != nil {
		return err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("Cannot parse IP from %s", host)
	}
	return m.Config.IPAllowed(ip)
}

func (m *Members) handleAlive(buf []byte, from net.Addr) {
	if err := m.ensureCanConnect(from); err != nil {
		m.Logger.Printf("[DEBUG] memberlist: Blocked Alive message: %s %s", err, LogAddress(from))
		return
	}
	var live Alive
	if err := Decode(buf, &live); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode Alive message: %s %s", err, LogAddress(from))
		return
	}
	if m.Config.IPMustBeChecked() {
		innerIP := net.IP(live.Addr)
		if innerIP != nil {
			if err := m.Config.IPAllowed(innerIP); err != nil {
				m.Logger.Printf("[DEBUG] memberlist: Blocked Alive.Addr=%s message from: %s %s", innerIP.String(), err, LogAddress(from))
				return
			}
		}
	}

	// For proto versions < 2, there is no Port provided. Mask old
	// behavior by using the configured Port
	if m.ProtocolVersion() < 2 || live.Port == 0 {
		live.Port = uint16(m.Config.BindPort)
	}

	m.AliveNode(&live, nil, false)
}

func (m *Members) handleDead(buf []byte, from net.Addr) {
	var d Dead
	if err := Decode(buf, &d); err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to Decode Dead message: %s %s", err, LogAddress(from))
		return
	}
	m.DeadNode(&d)
}

// handleUser is used to notify channels of incoming user data
func (m *Members) handleUser(buf []byte, from net.Addr) {
	d := m.Config.Delegate
	if d != nil {
		d.NotifyMsg(buf)
	}
}

// handleCompressed is used to unpack a Compressed message
func (m *Members) handleCompressed(buf []byte, from net.Addr, timestamp time.Time) {
	// Try to Decode the payload
	payload, err := DeCompressPayload(buf)
	if err != nil {
		m.Logger.Printf("[错误] memberlist: Failed to deCompress payload: %v %s", err, LogAddress(from))
		return
	}

	// Recursively handle the payload
	m.HandleCommand(payload, from, timestamp)
}

// encodeAndSendMsg is used to combine the encoding and sending steps
func (m *Members) encodeAndSendMsg(a Address, msgType MessageType, msg interface{}) error {
	out, err := Encode(msgType, msg)
	if err != nil {
		return err
	}
	if err := m.sendMsg(a, out.Bytes()); err != nil {
		return err
	}
	return nil
}

// sendMsg is used to send a message via packet to another host. It will
// opportunistically create a CompoundMsg and piggy back other Broadcasts.
func (m *Members) sendMsg(a Address, msg []byte) error {
	// Check if we can piggy back any messages
	bytesAvail := m.Config.UDPBufferSize - len(msg) - CompoundHeaderOverhead - LabelOverhead(m.Config.Label)
	if m.Config.EncryptionEnabled() && m.Config.GossipVerifyOutgoing {
		bytesAvail -= encryptOverhead(m.EncryptionVersion())
	}
	extra := m.getBroadcasts(CompoundOverhead, bytesAvail)

	// Fast path if nothing to piggypack
	if len(extra) == 0 {
		return m.RawSendMsgPacket(a, nil, msg)
	}

	// Join all the messages
	msgs := make([][]byte, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	// Create a compound message
	compound := MakeCompoundMessage(msgs)

	// Send the message
	return m.RawSendMsgPacket(a, nil, compound.Bytes())
}

// RawSendMsgPacket is used to send message via packet to another host without
// modification, other than Compression or encryption if enabled.
func (m *Members) RawSendMsgPacket(a Address, node *Node, msg []byte) error {
	if a.Name == "" && m.Config.RequireNodeNames {
		return errNodeNamesAreRequired
	}

	// Check if we have Compression enabled
	if m.Config.EnableCompression {
		buf, err := CompressPayload(msg)
		if err != nil {
			m.Logger.Printf("[WARN] memberlist: Failed to Compress payload: %v", err)
		} else {
			// 只有在压缩变小后，才使用压缩
			if buf.Len() < len(msg) {
				msg = buf.Bytes()
			}
		}
	}

	// Try to look up the destination node. Note this will only work if the
	// bare IP Address is used as the node name, which is not guaranteed.
	if node == nil {
		toAddr, _, err := net.SplitHostPort(a.Addr)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to parse Address %q: %v", a.Addr, err)
			return err
		}
		m.NodeLock.RLock()
		nodeState, ok := m.NodeMap[toAddr]
		m.NodeLock.RUnlock()
		if ok {
			node = &nodeState.Node
		}
	}

	// Add a CRC to the end of the payload if the recipient understands
	// ProtocolVersion >= 5
	if node != nil && node.PMax >= 5 {
		crc := crc32.ChecksumIEEE(msg)
		header := make([]byte, 5, 5+len(msg))
		header[0] = byte(HasCrcMsg)
		binary.BigEndian.PutUint32(header[1:], crc)
		msg = append(header, msg...)
	}

	// Check if we have encryption enabled
	if m.Config.EncryptionEnabled() && m.Config.GossipVerifyOutgoing {
		// Encrypt the payload
		var (
			primaryKey  = m.Config.Keyring.GetPrimaryKey()
			packetLabel = []byte(m.Config.Label)
			buf         bytes.Buffer
		)
		err := EncryptPayload(m.EncryptionVersion(), primaryKey, msg, packetLabel, &buf)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: Encryption of message failed: %v", err)
			return err
		}
		msg = buf.Bytes()
	}

	_, err := m.Transport.WriteToAddress(msg, a)
	return err
}

// RawSendMsgStream 是用来将一个信息流传到另一个主机上，不作任何修改
func (m *Members) RawSendMsgStream(conn net.Conn, sendBuf []byte, streamLabel string) error {
	// 是否允许压缩
	if m.Config.EnableCompression {
		compBuf, err := CompressPayload(sendBuf)
		if err != nil {
			m.Logger.Printf("[ERROR] memberlist: 压缩失败: %v", err)
		} else {
			sendBuf = compBuf.Bytes()
		}
	}

	// 是否允许加密
	if m.Config.EncryptionEnabled() && m.Config.GossipVerifyOutgoing {
		crypt, err := m.EncryptLocalState(sendBuf, streamLabel)
		if err != nil {
			m.Logger.Printf("[ERROR] memberlist: 加密失败: %v", err)
			return err
		}
		sendBuf = crypt
	}

	if n, err := conn.Write(sendBuf); err != nil {
		return err
	} else if n != len(sendBuf) {
		return fmt.Errorf("only %d of %d bytes written", n, len(sendBuf))
	}

	return nil
}

// sendUserMsg 是用来将用户信息流转到另一个主机。
func (m *Members) sendUserMsg(a Address, sendBuf []byte) error {
	if a.Name == "" && m.Config.RequireNodeNames {
		return errNodeNamesAreRequired
	}

	conn, err := m.Transport.DialAddressTimeout(a, m.Config.TCPTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	bufConn := bytes.NewBuffer(nil)
	if err := bufConn.WriteByte(byte(UserMsg)); err != nil {
		return err
	}

	header := UserMsgHeader{UserMsgLen: len(sendBuf)}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)
	if err := enc.Encode(&header); err != nil {
		return err
	}
	if _, err := bufConn.Write(sendBuf); err != nil {
		return err
	}

	return m.RawSendMsgStream(conn, bufConn.Bytes(), m.Config.Label)
}

// OK 发送本机数据、接收远端数据
func (m *Members) sendAndReceiveState(a Address, join bool) ([]PushNodeState, []byte, error) {
	if a.Name == "" && m.Config.RequireNodeNames {
		return nil, nil, errNodeNamesAreRequired
	}
	conn, err := m.Transport.DialAddressTimeout(a, m.Config.TCPTimeout)

	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	m.Logger.Printf("[DEBUG] memberlist: 初始化 push/pull 同步和: %s %s", a.Name, conn.RemoteAddr())

	// 发送自身状态,发送数据本身也设置了 TCP Timeout
	// net.go:234 ReadStream
	if err := m.sendLocalState(conn, join, m.Config.Label); err != nil {
		return nil, nil, err
	}
	//sendLocalState、ReadStream 一个发、一个收
	conn.SetDeadline(time.Now().Add(m.Config.TCPTimeout))
	//net.go:276 sendLocalState
	msgType, bufConn, dec, err := m.ReadStream(conn, m.Config.Label)
	if err != nil {
		return nil, nil, err
	}

	if msgType == ErrMsg {
		// 说明sendLocalState 发送过去的数据不对
		var resp errResp
		if err := dec.Decode(&resp); err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("remote error: %v", resp.Error)
	}

	if msgType != PushPullMsg {
		err := fmt.Errorf("无效的消息类型 (%d), 期待 PushPullMsg (%d) %s", msgType, PushPullMsg, LogConn(conn))
		return nil, nil, err
	}

	_, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
	return remoteNodes, userState, err
}

// sendLocalState 发送本地状态
func (m *Members) sendLocalState(conn net.Conn, join bool, streamLabel string) error {
	// 设置超时时间
	conn.SetDeadline(time.Now().Add(m.Config.TCPTimeout))

	m.NodeLock.RLock()
	localNodes := make([]PushNodeState, len(m.Nodes))
	for idx, n := range m.Nodes {
		localNodes[idx].Name = n.Name
		localNodes[idx].Addr = n.Addr
		localNodes[idx].Port = n.Port
		localNodes[idx].Incarnation = n.Incarnation
		localNodes[idx].State = n.State
		localNodes[idx].Meta = n.Meta
		localNodes[idx].Vsn = []uint8{
			n.PMin, n.PMax, n.PCur,
			n.DMin, n.DMax, n.DCur,
		}
	}
	m.NodeLock.RUnlock()

	// 获取委托的状态
	var userData []byte
	if m.Config.Delegate != nil {
		userData = m.Config.Delegate.LocalState(join)
	}

	bufConn := bytes.NewBuffer(nil)

	header := PushPullHeader{Nodes: len(localNodes), UserStateLen: len(userData), Join: join}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)

	if _, err := bufConn.Write([]byte{byte(PushPullMsg)}); err != nil {
		return err
	}
	// 消息类型【长度固定】、消息头【长度固定】、每个节点的信息、节点数据
	if err := enc.Encode(&header); err != nil {
		return err
	}
	for i := 0; i < header.Nodes; i++ {
		if err := enc.Encode(&localNodes[i]); err != nil {
			return err
		}
	}

	if userData != nil {
		if _, err := bufConn.Write(userData); err != nil {
			return err
		}
	}

	return m.RawSendMsgStream(conn, bufConn.Bytes(), streamLabel) // m.Config.Label
}

// ReadStream 解密、解压缩消息
func (m *Members) ReadStream(conn net.Conn, streamLabel string) (MessageType, io.Reader, *codec.Decoder, error) {
	var bufConn io.Reader = bufio.NewReader(conn)

	// 消息类型     EncryptMsg
	buf := [1]byte{0}
	if _, err := io.ReadFull(bufConn, buf[:]); err != nil {
		return 0, nil, nil, err
	}
	msgType := MessageType(buf[0]) // EncryptMsg

	if msgType == EncryptMsg {
		if !m.Config.EncryptionEnabled() {
			return 0, nil, nil, fmt.Errorf("远端状态是加密的，但本机加密信息没有配置")
		}

		plain, err := m.DecryptRemoteState(bufConn, streamLabel)
		if err != nil {
			return 0, nil, nil, err
		}

		msgType = MessageType(plain[0])
		bufConn = bytes.NewReader(plain[1:])
	} else if m.Config.EncryptionEnabled() && m.Config.GossipVerifyIncoming {
		return 0, nil, nil, fmt.Errorf("加密信息已配置,但远程的state没有加密")
	}

	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(bufConn, &hd)

	if msgType == CompressMsg {
		var c Compress
		if err := dec.Decode(&c); err != nil {
			return 0, nil, nil, err
		}
		decomp, err := DeCompressBuffer(&c)
		if err != nil {
			return 0, nil, nil, err
		}

		msgType = MessageType(decomp[0])
		bufConn = bytes.NewReader(decomp[1:])
		dec = codec.NewDecoder(bufConn, &hd)
	}

	return msgType, bufConn, dec, nil
}

// readRemoteState 从链接中读取远程状态
func (m *Members) readRemoteState(bufConn io.Reader, dec *codec.Decoder) (bool, []PushNodeState, []byte, error) {
	// PushPullHeader + localNodes + userData
	// 读 the push/pull 头
	var header PushPullHeader
	if err := dec.Decode(&header); err != nil {
		return false, nil, nil, err
	}

	remoteNodes := make([]PushNodeState, header.Nodes)

	// localNodes
	for i := 0; i < header.Nodes; i++ {
		if err := dec.Decode(&remoteNodes[i]); err != nil {
			return false, nil, nil, err
		}
	}
	// userData == UserState
	var userBuf []byte
	if header.UserStateLen > 0 {
		userBuf = make([]byte, header.UserStateLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserStateLen)
		if err == nil && bytes != header.UserStateLen {
			err = fmt.Errorf("读取userData 失败 (%d / %d)", bytes, header.UserStateLen)
		}
		if err != nil {
			return false, nil, nil, err
		}
	}

	// 当前版本是2 ,下边可以忽略了
	for idx := range remoteNodes {
		if m.ProtocolVersion() < 2 || remoteNodes[idx].Port == 0 {
			remoteNodes[idx].Port = uint16(m.Config.BindPort)
		}
	}

	return header.Join, remoteNodes, userBuf, nil
}

// mergeRemoteState 合并远程数据到本机
func (m *Members) mergeRemoteState(join bool, remoteNodes []PushNodeState, userBuf []byte) error {
	if err := m.VerifyProtocol(remoteNodes); err != nil {
		return err
	}

	// 如果有的话，调用合并委托。
	if join && m.Config.Merge != nil {
		nodes := make([]*Node, len(remoteNodes))
		for idx, n := range remoteNodes {
			nodes[idx] = &Node{
				Name:  n.Name,
				Addr:  n.Addr,
				Port:  n.Port,
				Meta:  n.Meta,
				State: n.State,
				PMin:  n.Vsn[0],
				PMax:  n.Vsn[1],
				PCur:  n.Vsn[2],
				DMin:  n.Vsn[3],
				DMax:  n.Vsn[4],
				DCur:  n.Vsn[5],
			}
		}
		if err := m.Config.Merge.NotifyMerge(nodes); err != nil {
			return err
		}
	}

	m.MergeState(remoteNodes)

	if userBuf != nil && m.Config.Delegate != nil {
		m.Config.Delegate.MergeRemoteState(userBuf, join)
	}
	return nil
}

// readUserMsg is used to Decode a UserMsg from a stream.
func (m *Members) readUserMsg(bufConn io.Reader, dec *codec.Decoder) error {
	// Read the user message header
	var header UserMsgHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	// Read the user message into a buffer
	var userBuf []byte
	if header.UserMsgLen > 0 {
		userBuf = make([]byte, header.UserMsgLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserMsgLen)
		if err == nil && bytes != header.UserMsgLen {
			err = fmt.Errorf(
				"Failed to read full user message (%d / %d)",
				bytes, header.UserMsgLen)
		}
		if err != nil {
			return err
		}

		d := m.Config.Delegate
		if d != nil {
			d.NotifyMsg(userBuf)
		}
	}

	return nil
}

// SendPingAndWaitForAck makes a stream connection to the given Address, sends
// a Ping, and waits for an ack. All of this is done as a series of blocking
// operations, given the Deadline. The bool return parameter is true if we
// we able to round trip a Ping to the other node.
func (m *Members) SendPingAndWaitForAck(a Address, ping Ping, Deadline time.Time) (bool, error) {
	if a.Name == "" && m.Config.RequireNodeNames {
		return false, errNodeNamesAreRequired
	}

	conn, err := m.Transport.DialAddressTimeout(a, Deadline.Sub(time.Now()))
	if err != nil {
		// If the node is actually Dead we expect this to fail, so we
		// shouldn't spam the logs with it. After this point, errors
		// with the connection are real, unexpected errors and should
		// get propagated up.
		return false, nil
	}
	defer conn.Close()
	conn.SetDeadline(Deadline)

	out, err := Encode(PingMsg, &ping)
	if err != nil {
		return false, err
	}

	if err = m.RawSendMsgStream(conn, out.Bytes(), m.Config.Label); err != nil {
		return false, err
	}

	msgType, _, dec, err := m.ReadStream(conn, m.Config.Label)
	if err != nil {
		return false, err
	}

	if msgType != AckRespMsg {
		return false, fmt.Errorf("Unexpected msgType (%d) from Ping %s", msgType, LogConn(conn))
	}

	var ack AckResp
	if err = dec.Decode(&ack); err != nil {
		return false, err
	}

	if ack.SeqNo != ping.SeqNo {
		return false, fmt.Errorf("Sequence number from ack (%d) doesn't match Ping (%d)", ack.SeqNo, ping.SeqNo)
	}

	return true, nil
}

// ------------------------------------------ TODO ---------------------------------------

// EncryptLocalState 在发送前 加密数据
func (m *Members) EncryptLocalState(sendBuf []byte, streamLabel string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(byte(EncryptMsg))

	sizeBuf := make([]byte, 4)
	encVsn := m.EncryptionVersion()
	encLen := EncryptedLength(encVsn, len(sendBuf))
	binary.BigEndian.PutUint32(sizeBuf, uint32(encLen))
	buf.Write(sizeBuf)
	// Authenticated Data is:
	//   [MessageType; byte] [messageLength; uint32] [stream_label; optional]
	//
	dataBytes := appendBytes(buf.Bytes()[:5], []byte(streamLabel))

	key := m.Config.Keyring.GetPrimaryKey()
	err := EncryptPayload(encVsn, key, sendBuf, dataBytes, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecryptRemoteState 解密state
func (m *Members) DecryptRemoteState(bufConn io.Reader, streamLabel string) ([]byte, error) {
	// Read in enough to determine message length
	cipherText := bytes.NewBuffer(nil)
	cipherText.WriteByte(byte(EncryptMsg))
	_, err := io.CopyN(cipherText, bufConn, 4)
	if err != nil {
		return nil, err
	}

	// Ensure we aren't asked to download too much. This is to guard against
	// an attack vector where a huge amount of state is sent
	moreBytes := binary.BigEndian.Uint32(cipherText.Bytes()[1:5])
	if moreBytes > maxPushStateBytes {
		return nil, fmt.Errorf("Remote node state is larger than limit (%d)", moreBytes)

	}

	//Start reporting the size before you cross the limit
	if moreBytes > uint32(math.Floor(.6*maxPushStateBytes)) {
		m.Logger.Printf("[WARN] memberlist: Remote node state size is (%d) limit is (%d)", moreBytes, maxPushStateBytes)
	}

	// Read in the rest of the payload
	_, err = io.CopyN(cipherText, bufConn, int64(moreBytes))
	if err != nil {
		return nil, err
	}

	// Decrypt the cipherText with some authenticated data
	//
	// Authenticated Data is:
	//
	//   [MessageType; byte] [messageLength; uint32] [label_data; optional]
	//
	dataBytes := appendBytes(cipherText.Bytes()[:5], []byte(streamLabel))
	cipherBytes := cipherText.Bytes()[5:]

	// Decrypt the payload
	keys := m.Config.Keyring.GetKeys()
	return DecryptPayload(keys, cipherBytes, dataBytes)
}
