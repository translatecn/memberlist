package memberlist

import (
	"math"
	"sync/atomic"
	"time"
)

// Suspicion 管理节点的可疑计时器，并提供一个接口，当我们获得更多关于节点可疑的独立确认时，可以加速超时。
type Suspicion struct {
	// N是我们看到的独立确认的数量。这必须使用原子指令更新，以防止与定时器回调争用。
	// 收到的质疑数

	SuspectAcceptNum int32

	// K是我们希望看到的独立确认的数量，以便将计时器驱动到它的最小值。
	// 质疑数上限,一旦超过设置dead
	SuspectMax int32 // 一般情况下是2

	// min 定时器最小值
	min time.Duration

	// max 定时器最大值
	max time.Duration

	// start 开始计时时的时间戳。这样我们就可以在更新期间计算持续时间，以达到我们想要的总时间。
	start time.Time

	// timer 是实现超时的底层计时器。
	timer *time.Timer

	// F是计时器到期时要调用的函数。我们持有它是因为有些情况下我们直接调用它。
	timeoutFn func()

	// 已确认给定节点可疑的“from”节点的映射。这可以防止重复计算。
	confirmations map[string]struct{}
}

// newSuspicion 返回一个从最大时间开始的定时器，在看到k个或更多的确认信息后，该定时器将驱动到最小时间。
// 从节点将被排除在确认之外，因为我们可能会得到我们自己的怀疑消息的流言蜚语。如果没有要求确认，将使用最小时间（k <= 0）。
func newSuspicion(from string, k int, min time.Duration, max time.Duration, fn func(int)) *Suspicion {
	s := &Suspicion{
		SuspectMax:             int32(k), // 一般是2
		min:           min,
		max:           max,
		confirmations: make(map[string]struct{}),
	}

	s.confirmations[from] = struct{}{}

	// 将确认次数传入超时功能，便于遥测。
	s.timeoutFn = func() {
		fn(int(atomic.LoadInt32(&s.SuspectAcceptNum))) // 超时了，发布dead消息
	}

	timeout := max
	if k < 1 {
		timeout = min
	}
	s.timer = time.AfterFunc(timeout, s.timeoutFn)

	s.start = time.Now()
	return s
}

// remainingSuspicionTime 返回在考虑一个节点死亡之前的剩余等待时间。
func remainingSuspicionTime(n, k int32, elapsed time.Duration, min, max time.Duration) time.Duration {
	frac := math.Log(float64(n)+1.0) / math.Log(float64(k)+1.0)
	raw := max.Seconds() - frac*(max.Seconds()-min.Seconds())
	timeout := time.Duration(math.Floor(1000.0*raw)) * time.Millisecond
	if timeout < min {
		timeout = min
	}

	// We have to take into account the amount of time that has passed so
	// far, so we get the right overall timeout.
	return timeout - elapsed
}

// Confirm 注册一个可能的新同行也确定给定节点是可疑的。
// 如果这是新的信息，则返回true；如果是重复的确认，则返回false；如果我们已经有足够的确认来达到最低限度。
func (s *Suspicion) Confirm(from string) bool {
	// 如果我们有足够的确认，那么就停止接受它们。
	if atomic.LoadInt32(&s.SuspectAcceptNum) >= s.SuspectMax {
		return false
	}

	// 只允许每个可能的对等体进行一次确认。
	if _, ok := s.confirmations[from]; ok {
		return false
	}
	// 记录
	s.confirmations[from] = struct{}{}// 又收到了来自其他节点的对目标节点的质疑

	// 考虑到当前的确认数，计算新的超时时间，并调整计时器。如果超时变成了负值，*并且我们可以干净地停止计时器，那么我们将从这里直接调用超时函数。
	n := atomic.AddInt32(&s.SuspectAcceptNum, 1)
	elapsed := time.Since(s.start) // 耗时
	//剩余的的质疑时间
	remaining := remainingSuspicionTime(n, s.SuspectMax, elapsed, s.min, s.max)
	if s.timer.Stop() { // 停止计时器，返回有没有停止成功，停止后，返回false
		if remaining > 0 {
			s.timer.Reset(remaining) // 收到消息后，重置重试时间
		} else {
			go s.timeoutFn()
		}
	}
	return true
}
