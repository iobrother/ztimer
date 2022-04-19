package ztimer

import (
	"sync"
	"sync/atomic"

	"github.com/iobrother/ztimer/pqueue"
	"github.com/iobrother/ztimer/util"
)

type timerTaskList struct {
	expiration  int64 // every timer task has the same expiration int this list
	taskCounter *util.AtomicInteger
	root        *timerTaskEntry
	sync.Mutex
}

func newTimerTaskList(taskCounter *util.AtomicInteger) *timerTaskList {
	// TimerTaskList forms a doubly linked cyclic list using a dummy root entry
	// root.next points to the head
	// root.prev points to the tail
	l := &timerTaskList{
		expiration:  0,
		taskCounter: taskCounter,
		root:        nil,
	}
	l.root = newTimerTaskEntry(nil, -1)
	l.root.next = l.root
	l.root.prev = l.root

	return l
}

// Add a timer task entry to this list
func (l *timerTaskList) Add(e *timerTaskEntry) {
	var done = false
	for !done {
		// Remove the timer task entry if it is already in any other list
		// We do this outside of the sync block below to avoid deadlocking.
		// We may retry until timerTaskEntry.list becomes null.
		e.Remove()

		if e.list == nil {
			l.Lock()
			// put the timer task entry to the end of the list. (root.prev points to the tail entry)
			tail := l.root.prev
			e.next = l.root
			e.prev = tail
			e.list = l
			tail.next = e
			l.root.prev = e
			l.taskCounter.IncrementAndGet()
			done = true
			l.Unlock()
		}
	}
}

// Remove the specified timer task entry from this list
func (l *timerTaskList) Remove(e *timerTaskEntry) {
	l.Lock()
	defer l.Unlock()
	if e.list == l {
		e.next.prev = e.prev
		e.prev.next = e.next
		e.next = nil
		e.prev = nil
		e.list = nil
		l.taskCounter.DecrementAndGet()
	}
}

func (l *timerTaskList) remove(e *timerTaskEntry) {
	if e.list == l {
		e.next.prev = e.prev
		e.prev.next = e.next
		e.next = nil
		e.prev = nil
		e.list = nil
		l.taskCounter.DecrementAndGet()
	}
}

// Flush Remove all task entries and apply the supplied function to each of them
func (l *timerTaskList) Flush(f func(*timerTaskEntry)) {
	l.Lock()
	defer l.Unlock()
	head := l.root.next
	for head != l.root {
		l.remove(head)
		f(head)
		head = l.root.next
	}
	l.SetExpiration(-1)
}

// SetExpiration Set the bucket's expiration time
// Returns true if the expiration time is changed
func (l *timerTaskList) SetExpiration(expirationMs int64) bool {
	return atomic.SwapInt64(&l.expiration, expirationMs) != expirationMs
}

// GetExpiration Get the bucket's expiration time
func (l *timerTaskList) GetExpiration() int64 {
	return atomic.LoadInt64(&l.expiration)
}

func (l *timerTaskList) GetDelay() int64 {
	delay := l.GetExpiration() - util.GetTimeMs()
	if delay < 0 {
		return 0
	}
	return delay
}

func (l *timerTaskList) CompareTo(other pqueue.Comparable) int {
	if l.GetExpiration() < other.(*timerTaskList).GetExpiration() {
		return -1
	} else if l.GetExpiration() > other.(*timerTaskList).GetExpiration() {
		return 1
	} else {
		return 0
	}
}

type timerTaskEntry struct {
	expirationMs int64 // in millisecond
	list         *timerTaskList
	next         *timerTaskEntry
	prev         *timerTaskEntry
	timerTask    *TimerTask // wraps TimerTask as a timerTaskEntry to add into timerTaskList
}

func newTimerTaskEntry(timerTask *TimerTask, expirationMs int64) *timerTaskEntry {
	e := &timerTaskEntry{}
	e.timerTask = timerTask
	e.expirationMs = expirationMs
	// if this timerTask is already held by an existing timer task entry,
	// setTimerTaskEntry will remove it.
	if timerTask != nil {
		timerTask.SetTimerTaskEntry(e)
	}
	return e
}

func (e *timerTaskEntry) Remove() {
	currentList := e.list

	// If remove is called when another thread is moving the entry from a task entry list to another,
	// this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
	// In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
	for currentList != nil {
		currentList.Remove(e)
		currentList = e.list
	}
}

func (e *timerTaskEntry) Cancelled() bool {
	return e.timerTask.GetTimerTaskEntry() != e
}
