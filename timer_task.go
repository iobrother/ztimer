package ztimer

import "sync"

type TimerTask struct {
	sync.Mutex
	delayMs        int64
	f              func()
	timerTaskEntry *timerTaskEntry
}

func (t *TimerTask) Cancel() {
	t.Lock()
	defer t.Unlock()
	if t.timerTaskEntry != nil {
		t.timerTaskEntry.Remove()
		t.timerTaskEntry = nil
	}
}

func (t *TimerTask) SetTimerTaskEntry(entry *timerTaskEntry) {
	t.Lock()
	defer t.Unlock()
	// if this timerTask is already held by an existing timer task entry,
	// we will remove such an entry first.
	if t.timerTaskEntry != nil && t.timerTaskEntry != entry {
		t.timerTaskEntry.Remove()
	}
	t.timerTaskEntry = entry
}

func (t *TimerTask) GetTimerTaskEntry() *timerTaskEntry {
	return t.timerTaskEntry
}

func (t *TimerTask) Run() {
	t.f()
}
