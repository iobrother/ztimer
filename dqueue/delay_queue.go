package dqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iobrother/ztimer/pqueue"
)

type Delayed interface {
	pqueue.Comparable
	GetDelay() int64
}

type DelayQueue struct {
	sync.Mutex
	q       *pqueue.PriorityQueue
	waiting int32
	wakeup  chan struct{}
}

func NewDelayQueue() *DelayQueue {
	return &DelayQueue{
		q:      pqueue.NewPriorityQueue(1),
		wakeup: make(chan struct{}),
	}
}

// Offer inserts the specified element into this queue.
func (dq *DelayQueue) Offer(d Delayed) {
	dq.Lock()
	dq.q.Push(d)
	first := dq.q.Front()
	dq.Unlock()

	if first.(Delayed) == d {
		if atomic.CompareAndSwapInt32(&dq.waiting, 1, 0) {
			dq.wakeup <- struct{}{}
		}
	}
}

// Poll retrieves and removes the head expired element, waiting if necessary until an element becomes expired.
func (dq *DelayQueue) Poll(ctx context.Context) Delayed {
	for {
		dq.Lock()

		var head Delayed
		var delay int64
		if e := dq.q.Front(); e != nil {
			head = e.(Delayed)
			delay = head.GetDelay()
			if delay <= 0 {
				dq.q.Remove(0)
				dq.Unlock()
				return head
			}
		}
		dq.Unlock()

		atomic.StoreInt32(&dq.waiting, 1)

		// No elements left.
		if head == nil {
			select {
			case <-dq.wakeup:
				// A new element is added.
				continue
			case <-ctx.Done():
				return nil
			}
		}

		// At least one element is pending.
		if head != nil {
			select {
			case <-dq.wakeup:
				// A new element that may expire earlier is added.
				continue
			case <-ctx.Done():
				return nil
			case <-time.After(time.Duration(delay) * time.Millisecond):
				if atomic.SwapInt32(&dq.waiting, 0) == 0 {
					<-dq.wakeup
				}
				continue
			}
		}
	}
}
