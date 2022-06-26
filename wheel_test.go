package turbine

import (
	"log"
	"math"
	"sync"
	"testing"
	"time"
)

func TestTurbine_Schedule(t *testing.T) {
	turbine := NewTurbine(time.Millisecond*50, 10000)
	turbine.Start()

	const TASKS = 20000000

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < TASKS; i++ {
		wg.Add(1)
		targetTime := (time.Duration(i) * time.Microsecond) + time.Millisecond*50
		startTime := time.Now()
		err := turbine.Schedule(targetTime, func() {
			wg.Done()
			waitedTime := time.Since(startTime)
			if time.Duration(math.Abs(float64(waitedTime-targetTime))) > time.Millisecond*150 {
				t.Errorf("expected %v, got %v", targetTime, waitedTime)
			}
		})
		if err != nil {
			panic(err)
		}
	}
	log.Printf("Scheduled %d tasks in %v", TASKS, time.Since(start))
	wg.Wait()
	turbine.Stop()
}
