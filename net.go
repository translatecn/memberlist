package memberlist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"hash/crc32"
	"io"
	"math"
	"net"
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
	AckRespMsg  // PING 确认
	SuspectMsg  // 怀疑消息
	AliveMsg    // 存活消息
	DeadMsg     // 死亡消息
	PushPullMsg // 推拉消息
	CompoundMsg // 复合消息
	UserMsg     // 用户消息、不处理
	CompressMsg // 压缩消息
	EncryptMsg  // 加密消息
	NAckRespMsg // 没有收到确认消息
	HasCrcMsg   // 校验消息
	ErrMsg      // 错误消息
)

const (
	// HasLabelMsg  有一个特意的高值，这样你就可以从EncryptionVersion标头（现在是0/1）
	// 和任何现有的MessageTypes中辨别出来。
	HasLabelMsg MessageType = 244
)

// CompressionType 压缩类型
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

type Ping struct {
	SeqNo uint32
	// 节点的发送，以便目标可以验证他们是预定的收件人。这是为了保护再次以新名字重新启动的代理。
	Node       string // 目标机器的名字
	SourceAddr []byte `codec:",omitempty"` // Source Address, used for a direct reply
	SourcePort uint16 `codec:",omitempty"` // Source Port, used for a direct reply
	SourceNode string `codec:",omitempty"` // 本机的名字
}
type TempPing struct {
	SeqNo      uint32
	Node       string
	SourceAddr string `codec:",omitempty"`
	SourcePort uint16 `codec:",omitempty"`
	SourceNode string `codec:",omitempty"`
}

func (p *Ping) PingCopy(SourceAddr string) TempPing {

	return TempPing{
		SeqNo:      p.SeqNo,
		Node:       p.Node,
		SourceAddr: SourceAddr,
		SourcePort: p.SourcePort,
		SourceNode: p.SourceNode,
	}
}

// IndirectPingReq   发送到间接节点
type IndirectPingReq struct {
	SeqNo  uint32
	Target []byte // 需要探活的节点IP
	Port   uint16 // 需要探活的节点端口

	Node string // 需要探活的节点

	Nack bool // 最大协议>=4 需要设置为TRUE    , 没有收到 ACK消息

	SourceAddr []byte `codec:",omitempty"` // Source Address, used for a direct reply
	SourcePort uint16 `codec:",omitempty"` // Source Port, used for a direct reply
	SourceNode string `codec:",omitempty"` // Source name, used for a direct reply
}

// AckResp ack response is sent for a Ping
type AckResp struct {
	SeqNo   uint32
	Payload []byte
}

// NAckResp response is sent for an indirect Ping when the pinger doesn't hear from
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

// Compress  包装数据、压缩算法
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

// readUserMsg 从TCP流中解码UserMsg
func (m *Members) readUserMsg(bufConn io.Reader, dec *codec.Decoder) error {
	var header UserMsgHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	var userBuf []byte
	if header.UserMsgLen > 0 {
		userBuf = make([]byte, header.UserMsgLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserMsgLen)
		if err == nil && bytes != header.UserMsgLen {
			err = fmt.Errorf(
				"读取完整的用户信息失败 (%d / %d)",
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

// SendPingAndWaitForAck 与给定的地址建立一个流连接，发送 Ping，并等待Ack。所有这些都是在给定的Deadline下，以一系列阻塞操作的方式完成的。
// 如果我们能够往返于Ping到另一个节点，则bool返回参数为true。
func (m *Members) SendPingAndWaitForAck(a pkg.Address, ping Ping, Deadline time.Time) (bool, error) {
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
		return false, fmt.Errorf("Unexpected msgType (%d) from Ping %s", msgType, pkg.LogConn(conn))
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

// ------------------------------------------ OVER ---------------------------------------

// encodeAndSendMsg 编码并发送消息
func (m *Members) encodeAndSendMsg(a pkg.Address, msgType MessageType, msg interface{}) error {
	out, err := Encode(msgType, msg)
	if err != nil {
		return err
	}
	if err := m.sendMsg(a, out.Bytes()); err != nil {
		return err
	}
	return nil
}

// 发送消息包到另一个节点,附加广播消息
func (m *Members) sendMsg(a pkg.Address, msg []byte) error {
	// Check if we can piggy back any messages
	// 检查我们是否可以捎带任何信息
	// 1400 - 消息长度 - 2 - [ label类型 (1byte) + label长度(1byte) + label大小 ] - 29
	bytesAvail := m.Config.UDPBufferSize - len(msg) - CompoundHeaderOverhead - LabelOverhead(m.Config.Label)
	if m.Config.EncryptionEnabled() && m.Config.GossipVerifyOutgoing {
		bytesAvail -= encryptOverhead(m.EncryptionVersion()) // Version: 1, IV: 12, Tag: 16   // 29
	}
	// 额外可以发送的消息，再一次UDP发包过程中
	extra := m.getBroadcasts(CompoundOverhead, bytesAvail) // 2  1303

	// Fast path if nothing to piggypack
	if len(extra) == 0 {
		return m.RawSendMsgPacket(a, nil, msg)
	}

	// Join all the messages
	msgs := make([][]byte, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	// 创建组合消息
	compound := MakeCompoundMessage(msgs)

	return m.RawSendMsgPacket(a, nil, compound.Bytes())
}

// SendUserMsg 是用来将用户信息流转到另一个主机。
func (m *Members) SendUserMsg(a pkg.Address, sendBuf []byte) error {
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

// RawSendMsgPacket UDP 是用来将一个信息流传到另一个主机上，不作任何修改
func (m *Members) RawSendMsgPacket(a pkg.Address, node *Node, msg []byte) error {
	//压缩+CRC+加密
	if a.Name == "" && m.Config.RequireNodeNames {
		return errNodeNamesAreRequired
	}

	// 是否允许压缩
	if m.Config.EnableCompression {
		buf, err := CompressPayload(msg)
		if err != nil {
			m.Logger.Printf("[WARN] memberlist: 压缩失败: %v", err)
		} else {
			// 只有在压缩变小后，才使用压缩
			if buf.Len() < len(msg) {
				msg = buf.Bytes()
			}
		}
	}

	// 尝试查找目标节点。注意这只有在裸露的IP地址被用作节点名称的情况下才会起作用，而这并不保证。
	if node == nil {
		toAddr, _, err := net.SplitHostPort(a.Addr)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: 解析地址失败%q: %v", a.Addr, err)
			return err
		}
		m.NodeLock.RLock()
		nodeState, ok := m.NodeMap[toAddr]
		m.NodeLock.RUnlock()
		if ok {
			node = &nodeState.Node
		}
	}

	// 添加CRC校验
	if node != nil && node.PMax >= 5 {
		crc := crc32.ChecksumIEEE(msg)
		header := make([]byte, 5, 5+len(msg))
		header[0] = byte(HasCrcMsg)
		binary.BigEndian.PutUint32(header[1:], crc)
		msg = append(header, msg...)
	}

	if m.Config.EncryptionEnabled() && m.Config.GossipVerifyOutgoing {
		var (
			primaryKey  = m.Config.Keyring.GetPrimaryKey()
			packetLabel = []byte(m.Config.Label)
			buf         bytes.Buffer
		)
		err := EncryptPayload(m.EncryptionVersion(), primaryKey, msg, packetLabel, &buf)
		if err != nil {
			m.Logger.Printf("[错误] memberlist: 加密消息失败: %v", err)
			return err
		}
		msg = buf.Bytes()
	}

	_, err := m.Transport.WriteToAddress(msg, a)
	return err
}

// RawSendMsgStream TCP 是用来将一个信息流传到另一个主机上，不作任何修改
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
	//   [MessageType; byte] [messageLength; uint32] [stream_Label; optional]
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
	//   [MessageType; byte] [messageLength; uint32] [Label_data; optional]
	//
	dataBytes := appendBytes(cipherText.Bytes()[:5], []byte(streamLabel))
	cipherBytes := cipherText.Bytes()[5:]

	// Decrypt the payload
	keys := m.Config.Keyring.GetKeys()
	return DecryptPayload(keys, cipherBytes, dataBytes)
}

// getNextMessage 使用LIFO，按优先级顺序返回下一个要处理的消息
func (m *Members) getNextMessage() (msgHandoff, bool) {
	m.msgQueueLock.Lock()
	defer m.msgQueueLock.Unlock()

	if el := m.HighPriorityMsgQueue.Back(); el != nil {
		m.HighPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	} else if el := m.LowPriorityMsgQueue.Back(); el != nil {
		m.LowPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	}
	return msgHandoff{}, false
}

// ensureCanConnect 确保IP能够链接
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
		return fmt.Errorf("不能解析IP %s", host)
	}
	return m.Config.IPAllowed(ip)
}
