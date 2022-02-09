package memberlist

// AliveDelegate 是用来让客户端参与处理一个节点的 "alive"消息。通过一个活着的消息更新该节点的状态。
type AliveDelegate interface {
	// NotifyAlive 从网络中接收到了某个节点的alive信息
	NotifyAlive(peer *Node) error
}
