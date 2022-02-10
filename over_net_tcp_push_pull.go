package memberlist

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist/pkg"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// ----------------------------------------- SERVER -------------------------------------------------

// StreamListen pull、push 模式, 处理来自其他节点的数据
func (m *Members) StreamListen() {
	for {
		select {
		case conn := <-m.Transport.GetStreamCh():
			go m.handleConn(conn) // pull、push 模式, 处理来自其他节点的数据
		case <-m.ShutdownCh:
			return
		}
	}
}

// handleConn 处理pull、push模式下的流链接
func (m *Members) handleConn(conn net.Conn) {
	defer conn.Close()
	m.Logger.Printf("[DEBUG] memberlist: 流连接 %s", pkg.LogConn(conn))

	conn.SetDeadline(time.Now().Add(m.Config.TCPTimeout))

	var (
		streamLabel string
		err         error
	)
	conn, streamLabel, err = RemoveLabelHeaderFromStream(conn)
	if err != nil {
		m.Logger.Printf("[错误] memberlist: 未能接收和删除流标签头: %s %s", err, pkg.LogConn(conn))
		return
	}

	if m.Config.SkipInboundLabelCheck {
		if streamLabel != "" {
			m.Logger.Printf("[错误] memberlist: 意外的流标签头: %s", pkg.LogConn(conn))
			return
		}
		streamLabel = m.Config.Label
	}

	if m.Config.Label != streamLabel {
		m.Logger.Printf("[错误] memberlist: 丢弃带有不可接受的标签的流 %q: %s", streamLabel, pkg.LogConn(conn))
		return
	}

	msgType, bufConn, dec, err := m.ReadStream(conn, streamLabel)
	if err != nil {
		if err != io.EOF {
			m.Logger.Printf("[错误] memberlist: 接受失败: %s %s", err, pkg.LogConn(conn))

			resp := errResp{err.Error()}
			out, err := Encode(ErrMsg, &resp)
			if err != nil {
				m.Logger.Printf("[错误] memberlist: 响应编码失败: %s", err)
				return
			}

			err = m.RawSendMsgStream(conn, out.Bytes(), streamLabel)
			if err != nil {
				m.Logger.Printf("[错误] memberlist: 发送失败: %s %s", err, pkg.LogConn(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case UserMsg:
		if err := m.readUserMsg(bufConn, dec); err != nil {
			m.Logger.Printf("[错误] memberlist: 接收用户消息失败: %s %s", err, pkg.LogConn(conn))
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
			m.Logger.Printf("[错误] memberlist: 读取远端state失败: %s %s", err, pkg.LogConn(conn))
			return
		}

		if err := m.sendLocalState(conn, join, streamLabel); err != nil {
			m.Logger.Printf("[错误] memberlist:发送本地state失败: %s %s", err, pkg.LogConn(conn))
			return
		}

		if err := m.mergeRemoteState(join, remoteNodes, userState); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed push/pull merge: %s %s", err, pkg.LogConn(conn))
			return
		}
	case PingMsg:
		var p Ping
		if err := dec.Decode(&p); err != nil {
			m.Logger.Printf("[错误] memberlist: Failed to Decode Ping: %s %s", err, pkg.LogConn(conn))
			return
		}

		if p.Node != "" && p.Node != m.Config.Name {
			m.Logger.Printf("[WARN] memberlist: Got Ping for unexpected node %s %s", p.Node, pkg.LogConn(conn))
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
			m.Logger.Printf("[错误] memberlist: Failed to send ack: %s %s", err, pkg.LogConn(conn))
			return
		}
	default:
		m.Logger.Printf("[错误] memberlist: 收到了无效的消息类型 (%d) %s", msgType, pkg.LogConn(conn))
	}
}

// ----------------------------------------- CLIENT -------------------------------------------------

// PushPull is invoked periodically to randomly perform a complete state
// exchange. Used to ensure a high level of convergence, but is also
// reasonably expensive as the entire state of this node is exchanged
// with the other node.
func (m *Members) PushPull() {
	// Get a random live node
	m.NodeLock.RLock()
	nodes := KRandomNodes(1, m.Nodes, func(n *NodeState) bool {
		return n.Name == m.Config.Name ||
			n.State != StateAlive
	})
	m.NodeLock.RUnlock()

	// If no Nodes, bail
	if len(nodes) == 0 {
		return
	}
	node := nodes[0]

	// Attempt a push pull
	if err := m.PushPullNode(node.FullAddress(), false); err != nil {
		m.Logger.Printf("[错误] memberlist: Push/Pull with %s failed: %s", node.Name, err)
	}
}

// PushPullNode 与一个特定的节点进行完整的状态交换。
func (m *Members) PushPullNode(a pkg.Address, join bool) error {

	remote, userState, err := m.sendAndReceiveState(a, join)
	if err != nil {
		return err
	}

	if err := m.mergeRemoteState(join, remote, userState); err != nil {
		return err
	}
	return nil
}

// OK 发送本机数据、接收远端数据
func (m *Members) sendAndReceiveState(a pkg.Address, join bool) ([]PushNodeState, []byte, error) {
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
		err := fmt.Errorf("无效的消息类型 (%d), 期待 PushPullMsg (%d) %s", msgType, PushPullMsg, pkg.LogConn(conn))
		return nil, nil, err
	}

	_, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
	return remoteNodes, userState, err
}

// ----------------------------------------- COMMON -------------------------------------------------

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
