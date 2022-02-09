package memberlist

type MergeDelegate interface {
	// NotifyMerge 远端节点合并到本地时,触发
	NotifyMerge(peers []*Node) error
}
