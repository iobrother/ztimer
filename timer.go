package ztimer

import (
	"context"
	"time"

	"github.com/iobrother/ztimer/dqueue"
	"github.com/iobrother/ztimer/util"
)

type Timer struct {
	tickMs      int64 // in millisecond
	wheelSize   int
	taskCounter *util.AtomicInteger // for counting the number of tasks pending execution concurrently
	delayQueue  *dqueue.DelayQueue
	timingWheel *timingWheel
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewTimer(tick time.Duration, wheelSize int) *Timer {
	t := new(Timer)
	t.tickMs = int64(tick / time.Millisecond)
	if t.tickMs <= 0 {
		panic("tick must be greater than or equal to 1ms")
	}
	t.wheelSize = wheelSize
	t.taskCounter = util.NewAtomicInteger()
	t.delayQueue = dqueue.NewDelayQueue()
	startMs := util.GetTimeMs()
	t.timingWheel = newTimingWheel(t.tickMs, t.wheelSize, startMs, t.taskCounter, t.delayQueue)
	t.ctx, t.cancel = context.WithCancel(context.Background())

	return t
}

func (t *Timer) Start() {
	go t.run()
}

func (t *Timer) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
}

// AfterFunc waits for the duration to elapse and then calls f
// in its own goroutine. It returns a TimerTask that can
// be used to cancel the call using its Cancel method.
func (t *Timer) AfterFunc(d time.Duration, f func()) *TimerTask {
	delayMs := int64(d / time.Millisecond)

	task := &TimerTask{
		delayMs: delayMs,
		f:       f,
	}

	entry := newTimerTaskEntry(task, task.delayMs+util.GetTimeMs())

	t.addTimerTaskEntry(entry)

	return task
}

// Size return the number of tasks pending execution.
func (t *Timer) Size() int64 {
	return t.taskCounter.Get()
}

func (t *Timer) run() {
	for {
		d := t.delayQueue.Poll(t.ctx)
		if d == nil {
			break
		}
		bucket := d.(*timerTaskList)
		t.timingWheel.AdvanceClock(bucket.GetExpiration()) // Advance timingWheel's current time to the expiration of the bucket.
		bucket.Flush(t.reinsert)                           // It causes these timer tasks to be performed.
	}
}

// Inserts a timerTaskEntry that wraps a TimerTask into timingWheel or run the TimerTask immediately.
// If the timer task has been cancelled, nothing happened.
func (t *Timer) addTimerTaskEntry(entry *timerTaskEntry) {
	if !t.timingWheel.Add(entry) {
		// Already expired or cancelled
		if !entry.Cancelled() {
			// Already expired, just run it immediately.
			// TODO: goroutine pool
			go func() {
				entry.timerTask.Run()
			}()
		}
	}
}

func (t *Timer) reinsert(entry *timerTaskEntry) {
	t.addTimerTaskEntry(entry)
}
