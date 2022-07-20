package turbine

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type TurbineState uint32

const (
	TurbineStateStopped TurbineState = iota
	TurbineStateRunning
	TurbineStateStopping
)

type Turbine struct {
	stopChan chan struct{}

	blades       []blade
	cursor       uint64
	tasksCounter uint64

	TaskRunner func(t int64, f func())

	unit  time.Duration
	state TurbineState

	periodicTasks []task
}

func (t *Turbine) getState() TurbineState {
	return TurbineState(atomic.LoadUint32((*uint32)(&t.state)))
}

func (t *Turbine) casState(old, new TurbineState) bool {
	return atomic.CompareAndSwapUint32((*uint32)(&t.state), uint32(old), uint32(new))
}

func (t *Turbine) setState(state TurbineState) {
	atomic.StoreUint32((*uint32)(&t.state), uint32(state))
}

func (t *Turbine) loadCursor() uint64 {
	return atomic.LoadUint64(&t.cursor)
}

type blade struct {
	mu sync.Mutex

	tasks []task
}

type taskType uint16

const (
	taskTypeAfter = iota
	taskTypePeriodic
)

type task struct {
	t     int64 // time.Now().UnixNano()
	after int64
	fn    func()

	tt taskType
}

func (b *blade) load(t task) {
	b.mu.Lock()
	b.tasks = append(b.tasks, t)
	b.mu.Unlock()
}

// NewTurbine creates a new Turbine with the given number of blades.
//
// Max Schedulable Time: (blades - 1) * unit
func NewTurbine(unit time.Duration, blades int) *Turbine {
	return &Turbine{
		unit:   unit,
		blades: make([]blade, blades),
		state:  TurbineStateStopped,
	}
}

var ErrOutOfRange = errors.New("out of range")

func (t *Turbine) calcBlade(after time.Duration) int {
	return int(t.loadCursor()+uint64(after/t.unit)) % len(t.blades)
}

func (t *Turbine) Schedule(after time.Duration, f func()) error {
	if after < t.unit {
		return ErrOutOfRange
	}

	if after > t.unit*time.Duration(len(t.blades)-1) {
		return ErrOutOfRange
	}

	atomic.AddUint64(&t.tasksCounter, 1)
	targetBlade := t.calcBlade(after)
	at := task{
		t:     time.Now().UnixNano(),
		fn:    f,
		tt:    taskTypeAfter,
		after: int64(after),
	}
	t.blades[targetBlade].load(at)

	return nil
}

func (t *Turbine) Every(period time.Duration, f func()) error {
	if period < t.unit {
		return ErrOutOfRange
	}

	if period > t.unit*time.Duration(len(t.blades)-1) {
		return ErrOutOfRange
	}

	atomic.AddUint64(&t.tasksCounter, 1)
	targetBlade := t.calcBlade(period)
	at := task{
		t:     time.Now().UnixNano(),
		fn:    f,
		tt:    taskTypePeriodic,
		after: int64(period),
	}
	t.blades[targetBlade].load(at)
	return nil
}

var ErrTurbineAlreadyRunning = errors.New("turbine already running")

func (t *Turbine) Start() error {
	if !t.casState(TurbineStateStopped, TurbineStateRunning) {
		return ErrTurbineAlreadyRunning
	}
	t.stopChan = make(chan struct{})
	go t.worker()
	return nil
}

var ErrTurbineAlreadyStoppingOrStopped = errors.New("turbine already stopping or stopped")

func (t *Turbine) Stop() error {
	if s := t.getState(); s != TurbineStateRunning {
		return ErrTurbineAlreadyStoppingOrStopped
	}
	if !t.casState(TurbineStateRunning, TurbineStateStopping) {
		return ErrTurbineAlreadyStoppingOrStopped
	}
	t.stopChan <- struct{}{}
	return nil
}

func (t *Turbine) worker() {
	ticker := time.NewTicker(t.unit)
	defer func() {
		ticker.Stop()
		close(t.stopChan)
		t.stopChan = nil
		t.setState(TurbineStateStopped)
	}()

	for {
		select {
		case <-t.stopChan:
			t.setState(TurbineStateStopped)
			return
		case <-ticker.C:
			t.update()
		}
	}
}

func (t *Turbine) update() {
	cursor := atomic.AddUint64(&t.cursor, 1)
	t.blades[cursor].mu.Lock()
	tasks := t.blades[cursor].tasks
	if t.TaskRunner == nil {
		for i := range tasks {
			tasks[i].fn()
		}
	} else {
		for i := range tasks {
			t.TaskRunner(tasks[i].t+tasks[i].after, tasks[i].fn)
		}
	}
	atomic.AddUint64(&t.tasksCounter, ^uint64(len(t.blades[cursor].tasks)-1))
	t.blades[cursor].tasks = t.blades[cursor].tasks[:0]
	t.periodicTasks = t.periodicTasks[:0]
	for i := range tasks {
		switch tasks[i].tt {
		case taskTypePeriodic:
			t.periodicTasks = append(t.periodicTasks, tasks[i])
		}
	}
	t.blades[cursor].mu.Unlock()

	// Reschedule periodic tasks
	now := time.Now().UnixNano()
	for i := range t.periodicTasks {
		last := t.periodicTasks[i].t
		t.periodicTasks[i].t = now
		diff := now - last - t.periodicTasks[i].after
		diff = diff / 2
		if (diff > 0 && diff < t.unit.Nanoseconds()) || (diff < 0 && -diff < t.unit.Nanoseconds()) {
			diff = 0 // set diff to 0 if abs(diff) < t.unit
		}
		after := time.Duration(t.periodicTasks[i].after - diff)
		if time.Duration(after) < t.unit {
			after = t.unit
		}
		targetBlade := t.calcBlade(after)
		t.blades[targetBlade].load(t.periodicTasks[i])
	}
}

func (t *Turbine) TasksCount() uint64 {
	return atomic.LoadUint64(&t.tasksCounter)
}
