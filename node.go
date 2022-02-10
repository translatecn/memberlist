package memberlist

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// LocalNode 返回本机的Node结构体信息
func (m *Members) LocalNode() *Node {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()
	state := m.NodeMap[m.Config.Name]
	return &state.Node
}

// UpdateNode 重新发送广播本地节点的信息
func (m *Members) UpdateNode(timeout time.Duration) error {
	var meta []byte
	if m.Config.Delegate != nil {
		meta = m.Config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("元信息超过了最大长度限制")
		}
	}

	m.NodeLock.RLock()
	state := m.NodeMap[m.Config.Name]
	m.NodeLock.RUnlock()

	a := Alive{
		Incarnation: m.NextIncarnation(),
		Node:        m.Config.Name,
		Addr:        state.Addr,
		Port:        state.Port,
		Meta:        meta,
		Vsn:         m.Config.BuildVsnArray(),
	}
	notifyCh := make(chan struct{})
	m.AliveNode(&a, notifyCh, true) // 发送成功,会给notifyCh发消息

	// 等待广播消息、或者超时
	if m.anyAlive() {
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		select {
		case <-notifyCh:
		case <-timeoutCh:
			return fmt.Errorf("广播更新超时")
		}
	}
	return nil
}

// Members returns 返回已知的存活节点,返回的结构体不能被修改。
func (m *Members) Members() []*Node {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()

	nodes := make([]*Node, 0, len(m.Nodes))
	for _, n := range m.Nodes {
		if !n.DeadOrLeft() {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

// GetNodeState OK
func (m *Members) GetNodeState(Addr string) NodeStateType {
	//Addr
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()

	n := m.NodeMap[Addr]
	return n.State
}

// GetNodeStateChange 获取节点上一次状态改变的时间
func (m *Members) GetNodeStateChange(Addr string) time.Time {
	m.NodeLock.RLock()
	defer m.NodeLock.RUnlock()

	n := m.NodeMap[Addr]
	return n.StateChange
}

// ChangeNode OK
func (m *Members) ChangeNode(Addr string, f func(*NodeState)) {
	m.NodeLock.Lock()
	defer m.NodeLock.Unlock()

	n := m.NodeMap[Addr]
	f(n)
}

// MergeState 合并远端的NodeState
func (m *Members) MergeState(remote []PushNodeState) {
	for _, r := range remote {
		switch r.State {
		case StateAlive:
			a := Alive{
				Incarnation: r.Incarnation,
				Node:        r.Name,
				Addr:        r.Addr,
				Port:        r.Port,
				Meta:        r.Meta,
				Vsn:         r.Vsn,
			}
			//m.AliveNode(&a, nil, true) // 存储节点state,广播存活消息
			m.AliveNode(&a, nil, false)
		case StateLeft:
			d := Dead{Incarnation: r.Incarnation, Node: r.Name, From: r.Name}
			m.DeadNode(&d)
		case StateDead:
			// 如果远程节点认为某个节点已经Dead，我们更愿意Suspect该节点，而不是立即宣布其死亡。
			fallthrough
		case StateSuspect:
			s := Suspect{Incarnation: r.Incarnation, Node: r.Name, From: m.Config.Name}
			m.SuspectNode(&s)
		}
	}
}

// AliveNode 广播某个存活着的节点信息
func (m *Members) AliveNode(a *Alive, notify chan struct{}, bootstrap bool) {
	m.NodeLock.Lock()
	defer m.NodeLock.Unlock()
	state, ok := m.NodeMap[a.Node]

	if m.hasLeft() && a.Node == m.Config.Name {
		//如果本节点在Leave()过程中，就不再发送本节点存活的消息了
		return
	}

	if len(a.Vsn) >= 3 {
		//protocol/delegate各个版本
		pMin := a.Vsn[0]
		pMax := a.Vsn[1]
		pCur := a.Vsn[2]
		if pMin == 0 || pMax == 0 || pMin > pMax {
			m.Logger.Printf("[WARN] memberlist:忽略存活消息 '%s' (%v:%d) 因为协议版本(s)错误: %d <= %d <= %d should be >0", a.Node, net.IP(a.Addr), a.Port, pMin, pCur, pMax)
			return
		}
	}

	// 调用Alive实现, 因为是nil不会走这里
	if m.Config.Alive != nil {
		if len(a.Vsn) < 6 {
			m.Logger.Printf("[WARN] memberlist:忽略存活消息'%s' (%v:%d) 因为Vsn长度不对", a.Node, net.IP(a.Addr), a.Port)
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
		if err := m.Config.Alive.NotifyAlive(node); err != nil {
			m.Logger.Printf("[WARN] memberlist: 忽略存活消息'%s': %s", a.Node, err)
			return
		}
	}

	// 检查我们是否以前从未见过这个节点，如果没有，那么就把这个节点储存在我们的节点地图中。
	var updatesNode bool
	if !ok {
		errCon := m.Config.IPAllowed(a.Addr)
		if errCon != nil {
			m.Logger.Printf("[WARN] memberlist: 拒绝节点 %s (%v): %s", a.Node, net.IP(a.Addr), errCon)
			return
		}
		state = &NodeState{
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
		m.NodeMap[a.Node] = state

		// 获得一个随机的偏移量。这对于确保故障检测边界平均较低是很重要的。如果所有的节点都做了追加，故障检测边界会非常高。
		n := len(m.Nodes) // 初始情况下为0
		offset := RandomOffset(n)

		m.Nodes = append(m.Nodes, state)
		//将最后一个节点与随机节点进行交换
		m.Nodes[offset], m.Nodes[n] = m.Nodes[n], m.Nodes[offset]

		// 更新节点数
		atomic.AddUint32(&m.numNodes, 1)
	} else {
		// Check if this Address is different than the existing node unless the old node is Dead.
		if !bytes.Equal([]byte(state.Addr), a.Addr) || state.Port != a.Port {
			errCon := m.Config.IPAllowed(a.Addr)
			if errCon != nil {
				m.Logger.Printf("[WARN] memberlist: Rejected IP update from %v to %v for node %s: %s", a.Node, state.Addr, net.IP(a.Addr), errCon)
				return
			}
			// If DeadNodeReclaimTime is configured, check if enough time has elapsed since the node died.
			canReclaim := (m.Config.DeadNodeReclaimTime > 0 &&
				time.Since(state.StateChange) > m.Config.DeadNodeReclaimTime)

			// Allow the Address to be updated if a Dead node is being replaced.
			if state.State == StateLeft || (state.State == StateDead && canReclaim) {
				m.Logger.Printf("[INFO] memberlist: Updating Address for left or failed node %s from %v:%d to %v:%d",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port)
				updatesNode = true
			} else {
				m.Logger.Printf("[错误] memberlist: Conflicting Address for %s. Mine: %v:%d Theirs: %v:%d Old state: %v",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port, state.State)

				// Inform the conflict delegate if provided
				if m.Config.Conflict != nil {
					other := Node{
						Name: a.Node,
						Addr: a.Addr,
						Port: a.Port,
						Meta: a.Meta,
					}
					m.Config.Conflict.NotifyConflict(&state.Node, &other)
				}
				return
			}
		}
	}

	isLocalNode := state.Name == m.Config.Name
	if a.Incarnation <= state.Incarnation && !isLocalNode && !updatesNode {
		// 1、Alive信息中的Incarnation<=该节点存储的Incarnation
		// 2、非本机
		// 3、不允许更新
		// 同时符合这三个条件,不进行下面逻辑
		return
	}

	if a.Incarnation < state.Incarnation && isLocalNode {
		// 1、Alive信息中的Incarnation<该节点存储的Incarnation
		// 2、本机
		// 同时符合这两个条件,不进行下面逻辑
		return
	}

	// 清理可能生效的计时器。
	delete(m.NodeTimers, a.Node)

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
		m.Logger.Printf("[WARN] memberlist: 拒绝Alive消息 '%s' (%v:%d) meta:(%v VS %v), vsn:(%v VS %v)", a.Node, net.IP(a.Addr), a.Port, a.Meta, state.Meta, a.Vsn, versions)
	} else {
		// 运行初走这里;
		m.EncodeBroadcastNotify(a.Node, AliveMsg, a, notify)
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
	if m.Config.Events != nil {
		if oldState == StateDead || oldState == StateLeft {
			// Dead/Left -> Alive, notify of join
			m.Config.Events.NotifyJoin(&state.Node)
		} else if !bytes.Equal(oldMeta, state.Meta) {
			m.Config.Events.NotifyUpdate(&state.Node)
		}
	}
}

// SuspectNode TODO
func (m *Members) SuspectNode(s *Suspect) {
	m.NodeLock.Lock()
	defer m.NodeLock.Unlock()
	state, ok := m.NodeMap[s.Node]

	if !ok {
		return
	}

	if s.Incarnation < state.Incarnation {
		return
	}

	// See if there's a Suspicion timer we can confirm. If the info is new
	// to us we will go ahead and re-gossip it. This allows for multiple
	// independent confirmations to flow even when a node probes a node
	// that's already Suspect.
	if timer, ok := m.NodeTimers[s.Node]; ok {
		if timer.Confirm(s.From) {
			m.EncodeBroadcast(s.Node, SuspectMsg, s)
		}
		return
	}

	if state.State != StateAlive {
		return
	}

	if state.Name == m.Config.Name {
		m.refute(state, s.Incarnation)
		m.Logger.Printf("[WARN] memberlist: 反驳质疑消息 (from: %s)", s.From)
		return
	} else {
		m.EncodeBroadcast(s.Node, SuspectMsg, s)
	}

	// Update the state
	state.Incarnation = s.Incarnation
	state.State = StateSuspect
	changeTime := time.Now()
	state.StateChange = changeTime

	// Setup a Suspicion timer. Given that we don't have any known phase
	// relationship with our peers, we set up k such that we hit the nominal
	// timeout two probe intervals short of what we expect given the Suspicion
	// multiplier.
	k := m.Config.SuspicionMult - 2

	// If there aren't enough Nodes to give the expected confirmations, just
	// set k to 0 to say that we don't expect any. Note we subtract 2 from n
	// here to take out ourselves and the node being probed.
	n := m.EstNumNodes()
	if n-2 < k {
		k = 0
	}

	// Compute the timeouts based on the size of the cluster.
	min := SuspicionTimeout(m.Config.SuspicionMult, n, m.Config.ProbeInterval)
	max := time.Duration(m.Config.SuspicionMaxTimeoutMult) * min
	fn := func(numConfirmations int) {
		var d *Dead

		m.NodeLock.Lock()
		state, ok := m.NodeMap[s.Node]
		timeout := ok && state.State == StateSuspect && state.StateChange == changeTime
		if timeout {
			d = &Dead{Incarnation: state.Incarnation, Node: state.Name, From: m.Config.Name}
		}
		m.NodeLock.Unlock()

		if timeout {
			m.Logger.Printf("[INFO] memberlist: Marking %s as failed, Suspect timeout reached (%d peer confirmations)",
				state.Name, numConfirmations)

			m.DeadNode(d)
		}
	}
	m.NodeTimers[s.Node] = newSuspicion(s.From, k, min, max, fn)
}

// DeadNode OK
func (m *Members) DeadNode(d *Dead) {
	m.NodeLock.Lock()
	defer m.NodeLock.Unlock()
	state, ok := m.NodeMap[d.Node]

	if !ok {
		return
	}

	// 忽略旧的Incarnation号
	if d.Incarnation < state.Incarnation {
		return
	}

	delete(m.NodeTimers, d.Node)

	if state.DeadOrLeft() {
		return
	}

	if state.Name == m.Config.Name {
		// 如果我们不离开，我们需要反驳
		if !m.hasLeft() {
			m.refute(state, d.Incarnation)
			m.Logger.Printf("[WARN] memberlist: 拒绝死亡消息 (from: %s)", d.From)
			return
		}
		// 如果我们要离开，我们就广播并等待
		m.EncodeBroadcastNotify(d.Node, DeadMsg, d, m.LeaveBroadcast)
	} else {
		m.EncodeBroadcast(d.Node, DeadMsg, d)
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

	if m.Config.Events != nil {
		m.Config.Events.NotifyLeave(&state.Node)
	}
}
