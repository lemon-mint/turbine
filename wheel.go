package turbine

import (
	"errors"
	"sync"
	"time"
)

type TurbineState uint32

const (
	TurbineStateStopped TurbineState = iota
	TurbineStateIdle
	TurbineStateRunning
	TurbineStateStopping
)

type Turbine struct {
	blades       []blade
	cursor       int
	tasksCounter uint64

	unit  time.Duration
	state TurbineState
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

func NewTurbine(unit time.Duration, size int) *Turbine {
	return &Turbine{
		unit:   unit,
		blades: make([]blade, size),
	}
}

var ErrOutOfRange = errors.New("out of range")

func (t *Turbine) Schedule(after time.Duration, f func()) error {
	if after > t.unit*time.Duration(len(t.blades)-1) {
		return ErrOutOfRange
	}

	targetBlade := (t.cursor + int(after/t.unit)) % len(t.blades)
	t.blades[targetBlade].load(f)

	return nil
}
