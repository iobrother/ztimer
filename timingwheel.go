package ztimer

import (
	"sync/atomic"
	"unsafe"

	"github.com/iobrother/ztimer/dqueue"
	"github.com/iobrother/ztimer/util"
)

type timingWheel struct {
	tickMs      int64 // in millisecond
	wheelSize   int
	interval    int64 // in millisecond
	currentTime int64 // timestamp in millisecond
	// the higher-level overflow wheel, overflowWheel can potentially be updated and read by two concurrent threads through Add().
	overflowWheel unsafe.Pointer      // type: *timingWheel
	taskCounter   *util.AtomicInteger // for counting the number of tasks pending execution concurrently
	queue         *dqueue.DelayQueue  // only need to enqueue bucket into this queue, because every bucket has the same expiration
	buckets       []*timerTaskList    // timerTaskList with the same expiration called bucket
}

func newTimingWheel(tickMs int64, wheelSize int, startMs int64, taskCounter *util.AtomicInteger, queue *dqueue.DelayQueue) *timingWheel {
	buckets := make([]*timerTaskList, wheelSize)
	for i := range buckets {
		buckets[i] = newTimerTaskList(taskCounter)
	}
	return &timingWheel{
		tickMs:        tickMs,
		wheelSize:     wheelSize,
		interval:      tickMs * int64(wheelSize),
		currentTime:   startMs - (startMs % tickMs), // rounding down to multiple of tickMs
		overflowWheel: nil,
		taskCounter:   taskCounter,
		queue:         queue,
		buckets:       buckets,
	}
}

func (tw *timingWheel) Add(entry *timerTaskEntry) bool {
	expiration := entry.expirationMs
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if entry.Cancelled() {
		// Canceled
		return false
	} else if expiration < currentTime+tw.tickMs {
		// Already expired
		return false
	} else if expiration < currentTime+tw.interval {
		// The expiration within the current wheel

		// Put in its own bucket
		virtualId := expiration / tw.tickMs
		// The entry with the same expiration will put into the same bucket
		bucket := tw.buckets[int(virtualId)%tw.wheelSize]
		bucket.Add(entry)

		// Set the bucket expiration time
		if bucket.SetExpiration(virtualId * tw.tickMs) {
			// The bucket needs to be enqueued because it was an expired bucket
			// We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
			// and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
			// will pass in the same value and hence return false, thus the bucket with the same expiration will not
			// be enqueued multiple times.

			tw.queue.Offer(bucket)
		}
		return true
	} else {
		// The expiration overflow the current wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.taskCounter,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*timingWheel)(overflowWheel).Add(entry)
	}
}

func (tw *timingWheel) AdvanceClock(timeMs int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if timeMs >= currentTime+tw.tickMs {
		currentTime = timeMs - (timeMs % tw.tickMs)
		atomic.StoreInt64(&tw.currentTime, currentTime)
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*timingWheel)(overflowWheel).AdvanceClock(currentTime)
		}
	}
}
