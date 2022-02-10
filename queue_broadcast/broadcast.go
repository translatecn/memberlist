package queue_broadcast

// Broadcast 通过gossip协议发送到集群中成员
type Broadcast interface {
	// Invalidates 检查 当前广播消息 是否使先前的广播无效
	Invalidates(b Broadcast) bool

	// Message 返回消息体
	Message() []byte

	// Finished 当信息不再被广播时被调用，无论是由于无效还是由于 达到发送限制
	Finished()
}
type MemberlistBroadcast struct {
	Node   string
	Msg    []byte
	Notify chan struct{}
}

// Invalidates 类型校验、验证消息是不是来自同一台机器
func (b *MemberlistBroadcast) Invalidates(other Broadcast) bool {
	mb, ok := other.(*MemberlistBroadcast)
	if !ok {
		return false
	}

	return b.Node == mb.Node
}

// Name ok
func (b *MemberlistBroadcast) Name() string {
	return b.Node
}

// Message OK
func (b *MemberlistBroadcast) Message() []byte {
	return b.Msg
}

func (b *MemberlistBroadcast) Finished() {
	select {
	case b.Notify <- struct{}{}:
	default:
	}
}
