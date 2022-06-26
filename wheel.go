package turbine

import "time"

type Turbine struct {
	unit time.Duration

	blades []blade
}

type blade struct {
}

func NewTurbine(unit time.Duration, size int) *Turbine {
	return &Turbine{
		unit:   unit,
		blades: make([]blade, size),
	}
}

func (t *Turbine) schedule() {

}
