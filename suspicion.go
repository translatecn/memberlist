package memberlist

import (
	"math"
	"sync/atomic"
	"time"
)

// suspicion 管理节点的可疑计时器，并提供一个接口，当我们获得更多关于节点可疑的独立确认时，可以加速超时。
type suspicion struct {
	// N是我们看到的独立确认的数量。这必须使用原子指令更新，以防止与定时器回调争用。
	n int32

	// K是我们希望看到的独立确认的数量，以便将计时器驱动到它的最小值。
	k int32

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

// newSuspicion returns a timer started with the max time, and that will drive
// to the min time after seeing k or more confirmations. The from node will be
// excluded from confirmations since we might get our own suspicion message
// gossiped back to us. The minimum time will be used if no confirmations are
// called for (k <= 0).
func newSuspicion(from string, k int, min time.Duration, max time.Duration, fn func(int)) *suspicion {
	s := &suspicion{
		k:             int32(k),
		min:           min,
		max:           max,
		confirmations: make(map[string]struct{}),
	}

	// Exclude the from node from any confirmations.
	s.confirmations[from] = struct{}{}

	// Pass the number of confirmations into the timeout function for
	// easy telemetry.
	s.timeoutFn = func() {
		fn(int(atomic.LoadInt32(&s.n)))
	}

	// If there aren't any confirmations to be made then take the min
	// time from the start.
	timeout := max
	if k < 1 {
		timeout = min
	}
	s.timer = time.AfterFunc(timeout, s.timeoutFn)

	// Capture the start time right after starting the timer above so
	// we should always err on the side of a little longer timeout if
	// there's any preemption that separates this and the step above.
	s.start = time.Now()
	return s
}

// remainingSuspicionTime takes the state variables of the suspicion timer and
// calculates the remaining time to wait before considering a node dead. The
// return value can be negative, so be prepared to fire the timer immediately in
// that case.
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
func (s *suspicion) Confirm(from string) bool {
	// If we've got enough confirmations then stop accepting them.
	if atomic.LoadInt32(&s.n) >= s.k {
		return false
	}

	// Only allow one confirmation from each possible peer.
	if _, ok := s.confirmations[from]; ok {
		return false
	}
	s.confirmations[from] = struct{}{}

	// Compute the new timeout given the current number of confirmations and
	// adjust the timer. If the timeout becomes negative *and* we can cleanly
	// stop the timer then we will call the timeout function directly from
	// here.
	n := atomic.AddInt32(&s.n, 1)
	elapsed := time.Since(s.start)
	remaining := remainingSuspicionTime(n, s.k, elapsed, s.min, s.max)
	if s.timer.Stop() {
		if remaining > 0 {
			s.timer.Reset(remaining)
		} else {
			go s.timeoutFn()
		}
	}
	return true
}
