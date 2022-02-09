package pkg

import (
	"sync"
	"time"
)

// Awareness
// 管理用于跟踪本地节点的估计运行状况的简单指标。运行状况是节点以软实时方式响应的主要能力，这是正确检查集群中其他节点的运行状况所必需的。
type Awareness struct {
	sync.RWMutex
	// max 超时范围的上限阈值是多少 ( 0 <= score < max).
	max int
	// 感知分数、低值是健康的>=0
	score int
}

// NewAwareness 节点健康对象
func NewAwareness(max int) *Awareness {
	return &Awareness{
		max:   max,
		score: 0,
	}
}

// ApplyDelta 以线程安全的方式获取给定的delta并将其应用到分数上。它还强制执行零的下限和最大的上限，因此，如果delta是在其中一个极端的轨道上，它可能不会改变总分。
func (a *Awareness) ApplyDelta(delta int) {
	a.Lock()
	a.score += delta
	if a.score < 0 {
		a.score = 0
	} else if a.score > (a.max - 1) {
		a.score = a.max - 1
	}
	a.Unlock()
}

// GetHealthScore 节点的健康程度 数字越小越好，而零意味着 "完全健康"。
func (a *Awareness) GetHealthScore() int {
	a.RLock()
	score := a.score
	a.RUnlock()
	return score
}

// ScaleTimeout 根据当前的分数读取持续时间。健康程度较低将导致更长的超时。
func (a *Awareness) ScaleTimeout(timeout time.Duration) time.Duration {
	a.RLock()
	score := a.score
	a.RUnlock()
	return timeout * (time.Duration(score) + 1)
}
