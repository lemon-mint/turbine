package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/lemon-mint/turbine"
)

var t = turbine.NewTurbine(time.Millisecond*50, 1000)

func main() {
	// LOCK_OS_THREAD
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Preheat the CPU
	BenchmarkTurbine(100000)
	BenchmarkTimeAfter(100000)

	for i := 0; i < 3; i++ {
		const N = 5000000
		fmt.Print("BenchmarkTurbine\t1\t")
		now := time.Now()
		BenchmarkTurbine(N)
		fmt.Printf("%d ns/op\n", int(time.Since(now).Nanoseconds()))
		fmt.Print("BenchmarkTimeAfter\t1\t")
		now = time.Now()
		BenchmarkTimeAfter(N)
		fmt.Printf("%d ns/op\n", int(time.Since(now).Nanoseconds()))
	}
}

func BenchmarkTurbine(n int) {
	t.Start()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		targetTime := (time.Duration(i) * time.Second / time.Duration(n)) + time.Millisecond*50
		err := t.Schedule(targetTime, func() {
			wg.Done()
		})
		if err != nil {
			panic(err)
		}
	}
	wg.Wait()
	t.Stop()
}

func BenchmarkTimeAfter(n int) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		targetTime := (time.Duration(i) * time.Second / time.Duration(n)) + time.Millisecond*50
		wg.Add(1)
		time.AfterFunc(targetTime, func() {
			wg.Done()
		})
	}
	wg.Wait()
}
