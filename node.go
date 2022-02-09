package memberlist

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// LocalNode is used to return the local Node
func (m *Members) LocalNode() *Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	state := m.nodeMap[m.config.Name]
	return &state.Node
}

// UpdateNode is used to trigger re-advertising the local node. This is
// primarily used with a Delegate to support dynamic updates to the local
// meta data.  This will block until the update message is successfully
// broadcasted to a member of the cluster, if any exist or until a specified
// timeout is reached.
func (m *Members) UpdateNode(timeout time.Duration) error {
	// Get the node meta data
	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	// Get the existing node
	m.nodeLock.RLock()
	state := m.nodeMap[m.config.Name]
	m.nodeLock.RUnlock()

	// Format a new alive message
	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        state.Addr,
		Port:        state.Port,
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	notifyCh := make(chan struct{})
	m.aliveNode(&a, notifyCh, true)

	// Wait for the broadcast or a timeout
	if m.anyAlive() {
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		select {
		case <-notifyCh:
		case <-timeoutCh:
			return fmt.Errorf("timeout waiting for update broadcast")
		}
	}
	return nil
}

// Members returns 返回已知的存活节点,返回的结构体不能被修改。
func (m *Members) Members() []*Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

func (m *Members) getNodeState(addr string) NodeStateType {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.State
}

func (m *Members) getNodeStateChange(addr string) time.Time {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.StateChange
}

func (m *Members) changeNode(addr string, f func(*nodeState)) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	n := m.nodeMap[addr]
	f(n)
}

// mergeState 合并远端的NodeState
func (m *Members) mergeState(remote []pushNodeState) {
	for _, r := range remote {
		switch r.State {
		case StateAlive:
			a := alive{
				Incarnation: r.Incarnation,
				Node:        r.Name,
				Addr:        r.Addr,
				Port:        r.Port,
				Meta:        r.Meta,
				Vsn:         r.Vsn,
			}
			//m.aliveNode(&a, nil, true) // 存储节点state,广播存活消息
			m.aliveNode(&a, nil, false)

		case StateLeft:
			d := dead{Incarnation: r.Incarnation, Node: r.Name, From: r.Name}
			m.deadNode(&d)
		case StateDead:
			// 如果远程节点认为某个节点已经dead，我们更愿意suspect该节点，而不是立即宣布其死亡。
			fallthrough
		case StateSuspect:
			s := suspect{Incarnation: r.Incarnation, Node: r.Name, From: m.config.Name}
			m.suspectNode(&s)
		}
	}
}

// aliveNode 广播某个存活着的节点信息
func (m *Members) aliveNode(a *alive, notify chan struct{}, bootstrap bool) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[a.Node]

	if m.hasLeft() && a.Node == m.config.Name {
		//如果本节点在Leave()过程中，就不再发送本节点存活的消息了
		return
	}

	if len(a.Vsn) >= 3 {
		//protocol/delegate各个版本
		pMin := a.Vsn[0]
		pMax := a.Vsn[1]
		pCur := a.Vsn[2]
		if pMin == 0 || pMax == 0 || pMin > pMax {
			m.logger.Printf("[WARN] memberlist:忽略存活消息 '%s' (%v:%d) 因为协议版本(s)错误: %d <= %d <= %d should be >0", a.Node, net.IP(a.Addr), a.Port, pMin, pCur, pMax)
			return
		}
	}

	// 调用alive实现, 因为是nil不会走这里
	if m.config.Alive != nil {
		if len(a.Vsn) < 6 {
			m.logger.Printf("[WARN] memberlist:忽略存活消息'%s' (%v:%d) 因为Vsn长度不对", a.Node, net.IP(a.Addr), a.Port)
			return
		}
		node := &Node{
			Name: a.Node,
			Addr: a.Addr,
			Port: a.Port,
			Meta: a.Meta,
			PMin: a.Vsn[0],
			PMax: a.Vsn[1],
			PCur: a.Vsn[2],
			DMin: a.Vsn[3],
			DMax: a.Vsn[4],
			DCur: a.Vsn[5],
		}
		if err := m.config.Alive.NotifyAlive(node); err != nil {
			m.logger.Printf("[WARN] memberlist: 忽略存活消息'%s': %s", a.Node, err)
			return
		}
	}

	// 检查我们是否以前从未见过这个节点，如果没有，那么就把这个节点储存在我们的节点地图中。
	var updatesNode bool
	if !ok {
		errCon := m.config.IPAllowed(a.Addr)
		if errCon != nil {
			m.logger.Printf("[WARN] memberlist: 拒绝节点 %s (%v): %s", a.Node, net.IP(a.Addr), errCon)
			return
		}
		state = &nodeState{
			Node: Node{
				Name: a.Node, // ls-2018.local
				Addr: a.Addr,
				Port: a.Port, // 8000
				Meta: a.Meta, // nil
			},
			State: StateDead,
		}
		if len(a.Vsn) > 5 {
			state.PMin = a.Vsn[0]
			state.PMax = a.Vsn[1]
			state.PCur = a.Vsn[2]
			state.DMin = a.Vsn[3]
			state.DMax = a.Vsn[4]
			state.DCur = a.Vsn[5]
		}
		// ls-2018.local -> NodeState
		m.nodeMap[a.Node] = state

		// 获得一个随机的偏移量。这对于确保故障检测边界平均较低是很重要的。如果所有的节点都做了追加，故障检测边界会非常高。
		n := len(m.nodes) // 初始情况下为0
		offset := randomOffset(n)

		m.nodes = append(m.nodes, state)
		//将最后一个节点与随机节点进行交换
		m.nodes[offset], m.nodes[n] = m.nodes[n], m.nodes[offset]

		// 更新节点数
		atomic.AddUint32(&m.numNodes, 1)
	} else {
		// Check if this address is different than the existing node unless the old node is dead.
		if !bytes.Equal([]byte(state.Addr), a.Addr) || state.Port != a.Port {
			errCon := m.config.IPAllowed(a.Addr)
			if errCon != nil {
				m.logger.Printf("[WARN] memberlist: Rejected IP update from %v to %v for node %s: %s", a.Node, state.Addr, net.IP(a.Addr), errCon)
				return
			}
			// If DeadNodeReclaimTime is configured, check if enough time has elapsed since the node died.
			canReclaim := (m.config.DeadNodeReclaimTime > 0 &&
				time.Since(state.StateChange) > m.config.DeadNodeReclaimTime)

			// Allow the address to be updated if a dead node is being replaced.
			if state.State == StateLeft || (state.State == StateDead && canReclaim) {
				m.logger.Printf("[INFO] memberlist: Updating address for left or failed node %s from %v:%d to %v:%d",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port)
				updatesNode = true
			} else {
				m.logger.Printf("[错误] memberlist: Conflicting address for %s. Mine: %v:%d Theirs: %v:%d Old state: %v",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port, state.State)

				// Inform the conflict delegate if provided
				if m.config.Conflict != nil {
					other := Node{
						Name: a.Node,
						Addr: a.Addr,
						Port: a.Port,
						Meta: a.Meta,
					}
					m.config.Conflict.NotifyConflict(&state.Node, &other)
				}
				return
			}
		}
	}

	isLocalNode := state.Name == m.config.Name
	if a.Incarnation <= state.Incarnation && !isLocalNode && !updatesNode {
		// 1、alive信息中的Incarnation<=该节点存储的Incarnation
		// 2、非本机
		// 3、不允许更新
		// 同时符合这三个条件,不进行下面逻辑
		return
	}

	if a.Incarnation < state.Incarnation && isLocalNode {
		// 1、alive信息中的Incarnation<该节点存储的Incarnation
		// 2、本机
		// 同时符合这两个条件,不进行下面逻辑
		return
	}

	// 清理可能生效的计时器。
	delete(m.nodeTimers, a.Node)

	oldState := state.State
	oldMeta := state.Meta

	if !bootstrap && isLocalNode { // 运行初是     true,true,不会走这里
		versions := []uint8{
			state.PMin, state.PMax, state.PCur,
			state.DMin, state.DMax, state.DCur,
		}
		// TODO
		// If the Incarnation is the same, we need special handling, since it
		// possible for the following situation to happen:
		// 1) Start with configuration C, join cluster
		// 2) Hard fail / Kill / Shutdown
		// 3) Restart with configuration C', join cluster
		// 在这种情况下，其他节点和本地节点看到的是同一个incarnation，但数值可能不一样。
		// 出于这个原因，我们总是需要做一个一致性检查。在大多数情况下，我们只是忽略，但我们可能需要反驳。
		if a.Incarnation == state.Incarnation && bytes.Equal(a.Meta, state.Meta) && bytes.Equal(a.Vsn, versions) {
			return
		}
		m.refute(state, a.Incarnation)
		m.logger.Printf("[WARN] memberlist: 拒绝alive消息 '%s' (%v:%d) meta:(%v VS %v), vsn:(%v VS %v)", a.Node, net.IP(a.Addr), a.Port, a.Meta, state.Meta, a.Vsn, versions)
	} else {
		// 运行初走这里;
		m.encodeBroadcastNotify(a.Node, aliveMsg, a, notify)
		// 更新数据
		if len(a.Vsn) > 0 {
			state.PMin = a.Vsn[0]
			state.PMax = a.Vsn[1]
			state.PCur = a.Vsn[2]
			state.DMin = a.Vsn[3]
			state.DMax = a.Vsn[4]
			state.DCur = a.Vsn[5]
		}
		state.Incarnation = a.Incarnation
		state.Meta = a.Meta
		state.Addr = a.Addr
		state.Port = a.Port
		if state.State != StateAlive {
			// 初始状态是StateDead
			state.State = StateAlive
			state.StateChange = time.Now()
		}
	}

	// 通知 delegate 一旦有任何相关的更新信息
	if m.config.Events != nil {
		if oldState == StateDead || oldState == StateLeft {
			// Dead/Left -> Alive, notify of join
			m.config.Events.NotifyJoin(&state.Node)
		} else if !bytes.Equal(oldMeta, state.Meta) {
			m.config.Events.NotifyUpdate(&state.Node)
		}
	}
}

// OK
func (m *Members) suspectNode(s *suspect) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[s.Node]

	if !ok {
		return
	}

	if s.Incarnation < state.Incarnation {
		return
	}

	// See if there's a suspicion timer we can confirm. If the info is new
	// to us we will go ahead and re-gossip it. This allows for multiple
	// independent confirmations to flow even when a node probes a node
	// that's already suspect.
	if timer, ok := m.nodeTimers[s.Node]; ok {
		if timer.Confirm(s.From) {
			m.encodeBroadcast(s.Node, suspectMsg, s)
		}
		return
	}

	if state.State != StateAlive {
		return
	}

	if state.Name == m.config.Name {
		m.refute(state, s.Incarnation)
		m.logger.Printf("[WARN] memberlist: 反驳质疑消息 (from: %s)", s.From)
		return
	} else {
		m.encodeBroadcast(s.Node, suspectMsg, s)
	}

	// Update the state
	state.Incarnation = s.Incarnation
	state.State = StateSuspect
	changeTime := time.Now()
	state.StateChange = changeTime

	// Setup a suspicion timer. Given that we don't have any known phase
	// relationship with our peers, we set up k such that we hit the nominal
	// timeout two probe intervals short of what we expect given the suspicion
	// multiplier.
	k := m.config.SuspicionMult - 2

	// If there aren't enough nodes to give the expected confirmations, just
	// set k to 0 to say that we don't expect any. Note we subtract 2 from n
	// here to take out ourselves and the node being probed.
	n := m.estNumNodes()
	if n-2 < k {
		k = 0
	}

	// Compute the timeouts based on the size of the cluster.
	min := suspicionTimeout(m.config.SuspicionMult, n, m.config.ProbeInterval)
	max := time.Duration(m.config.SuspicionMaxTimeoutMult) * min
	fn := func(numConfirmations int) {
		var d *dead

		m.nodeLock.Lock()
		state, ok := m.nodeMap[s.Node]
		timeout := ok && state.State == StateSuspect && state.StateChange == changeTime
		if timeout {
			d = &dead{Incarnation: state.Incarnation, Node: state.Name, From: m.config.Name}
		}
		m.nodeLock.Unlock()

		if timeout {
			m.logger.Printf("[INFO] memberlist: Marking %s as failed, suspect timeout reached (%d peer confirmations)",
				state.Name, numConfirmations)

			m.deadNode(d)
		}
	}
	m.nodeTimers[s.Node] = newSuspicion(s.From, k, min, max, fn)
}

// OK
func (m *Members) deadNode(d *dead) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[d.Node]

	if !ok {
		return
	}

	// 忽略旧的Incarnation号
	if d.Incarnation < state.Incarnation {
		return
	}

	delete(m.nodeTimers, d.Node)

	if state.DeadOrLeft() {
		return
	}

	if state.Name == m.config.Name {
		// 如果我们不离开，我们需要反驳
		if !m.hasLeft() {
			m.refute(state, d.Incarnation)
			m.logger.Printf("[WARN] memberlist: 拒绝死亡消息 (from: %s)", d.From)
			return
		}
		// 如果我们要离开，我们就广播并等待
		m.encodeBroadcastNotify(d.Node, deadMsg, d, m.leaveBroadcast)
	} else {
		m.encodeBroadcast(d.Node, deadMsg, d)
	}

	// 更新Incarnation
	state.Incarnation = d.Incarnation

	// 如果死亡信息是由节点自己发送的，则将其标记为Left，而不是死亡。
	// TODO 这没有很明白
	if d.Node == d.From {
		state.State = StateLeft
	} else {
		state.State = StateDead
	}
	state.StateChange = time.Now()

	if m.config.Events != nil {
		m.config.Events.NotifyLeave(&state.Node)
	}
}
