package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/iobrother/ztimer"
)

func main() {
	t := ztimer.NewTimer(time.Millisecond, 20)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		index := i
		t.AfterFunc(time.Duration(i)*time.Second, func() {
			fmt.Printf("timer task %d is executed\n", index)
			wg.Done()
		})
	}
	t.Start()
	wg.Wait()
	t.Stop()
}
