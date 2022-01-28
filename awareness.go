package memberlist

import (
	"sync"
	"time"
)

// awareness manages a simple metric for tracking the estimated health of the
// local node. Health is primary the node's ability to respond in the soft
// real-time manner required for correct health checking of other nodes in the
// cluster.
// 管理用于跟踪本地节点的估计运行状况的简单指标。运行状况是节点以软实时方式响应的主要能力，这是正确检查集群中其他节点的运行状况所必需的。
type awareness struct {
	sync.RWMutex

	// max 超时范围的上限阈值是多少 ( 0 <= score < max).
	max int

	// 感知分数、低值是健康的>=0
	score int
}

// newAwareness 感知对象
func newAwareness(max int) *awareness {
	return &awareness{
		max:   max,
		score: 0,
	}
}

// ApplyDelta takes the given delta and applies it to the score in a thread-safe
// manner. It also enforces a floor of zero and a max of max, so deltas may not
// change the overall score if it's railed at one of the extremes.
func (a *awareness) ApplyDelta(delta int) {
	a.Lock()
	a.score += delta
	if a.score < 0 {
		a.score = 0
	} else if a.score > (a.max - 1) {
		a.score = (a.max - 1)
	}
	a.Unlock()
}

// GetHealthScore returns the raw health score.
func (a *awareness) GetHealthScore() int {
	a.RLock()
	score := a.score
	a.RUnlock()
	return score
}

// ScaleTimeout takes the given duration and scales it based on the current
// score. Less healthyness will lead to longer timeouts.
func (a *awareness) ScaleTimeout(timeout time.Duration) time.Duration {
	a.RLock()
	score := a.score
	a.RUnlock()
	return timeout * (time.Duration(score) + 1)
}
