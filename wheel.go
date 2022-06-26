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
	blades       []blade
	cursor       uint64
	tasksCounter uint64

	unit  time.Duration
	state TurbineState

	stopChan chan struct{}

	TaskRunner func(f func())
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

	tasks []func()
}

func (b *blade) load(f func()) {
	b.mu.Lock()
	b.tasks = append(b.tasks, f)
	b.mu.Unlock()
}

// NewTurbine creates a new Turbine with the given number of blades.
//
// Max Schedulable Time: (blades - 1) * unit
func NewTurbine(unit time.Duration, blades int) *Turbine {
	return &Turbine{
		unit:   unit,
		blades: make([]blade, blades),
	}
}

var ErrOutOfRange = errors.New("out of range")

func (t *Turbine) Schedule(after time.Duration, f func()) error {
	if after > t.unit*time.Duration(len(t.blades)-1) {
		return ErrOutOfRange
	}

	atomic.AddUint64(&t.tasksCounter, 1)
	targetBlade := int(t.loadCursor()+uint64(after/t.unit)) % len(t.blades)
	t.blades[targetBlade].load(f)

	return nil
}

var ErrTurbineAlreadyRunning = errors.New("turbine already running")

func (t *Turbine) Start() error {
	if !t.casState(TurbineStateStopped, TurbineStateRunning) {
		return ErrTurbineAlreadyRunning
	}
	if t.TaskRunner == nil {
		t.TaskRunner = func(f func()) { f() }
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
	defer ticker.Stop()
	defer close(t.stopChan)
	defer t.setState(TurbineStateStopped)

	for {
		select {
		case <-t.stopChan:
			t.setState(TurbineStateStopped)
			return
		case <-ticker.C:
			cursor := atomic.AddUint64(&t.cursor, 1)
			t.blades[cursor].mu.Lock()
			for i := range t.blades[cursor].tasks {
				t.TaskRunner(t.blades[cursor].tasks[i])
			}
			atomic.AddUint64(&t.tasksCounter, ^uint64(len(t.blades[cursor].tasks)-1))
			t.blades[cursor].tasks = t.blades[cursor].tasks[:0]
			t.blades[cursor].mu.Unlock()
		}
	}
}
