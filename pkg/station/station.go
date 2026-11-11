// Package station simulates a single manufacturing station (PLC) with a
// state machine, cycle timing, fault injection, scrap, and a Modbus register map.
package station

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/hareshgoyal06/andon/pkg/modbus"
)

// State of a station.
type State uint16

const (
	Idle State = iota
	Running
	Faulted
	Blocked
)

func (s State) String() string {
	switch s {
	case Idle:
		return "idle"
	case Running:
		return "running"
	case Faulted:
		return "faulted"
	case Blocked:
		return "blocked"
	}
	return "unknown"
}

// Register addresses (see README register map).
const (
	RegState       uint16 = 0
	RegCycleMs     uint16 = 1
	RegPartCount   uint16 = 2
	RegTempC10     uint16 = 3
	RegFaultCode   uint16 = 4
	RegScrapCount  uint16 = 5
	RegTargetCycle uint16 = 10 // RW
	RegFaultInject uint16 = 11 // RW

	// NumRegs is the size of the register bank.
	NumRegs = 16
)

// Config tunes the simulation.
type Config struct {
	ID            int
	TargetCycleMs uint16
	FaultProb     float64       // probability of a spontaneous fault per cycle
	ScrapProb     float64       // probability a cycle produces scrap instead of a good part
	FaultDuration time.Duration // how long a fault lasts before auto-recovering
	Jitter        float64       // +/- fraction of target cycle time
}

// DefaultConfig returns a reasonable config for station id.
func DefaultConfig(id int) Config {
	return Config{
		ID:            id,
		TargetCycleMs: 1000,
		FaultProb:     0.01,
		ScrapProb:     0.02,
		FaultDuration: 5 * time.Second,
		Jitter:        0.15,
	}
}

// Station is a simulated PLC. It implements modbus.RegisterStore.
type Station struct {
	cfg Config

	mu    sync.RWMutex
	regs  [NumRegs]uint16
	rng   *rand.Rand
	temp  float64
	fault time.Time // when the current fault clears
}

// New creates a station in the Idle state.
func New(cfg Config) *Station {
	s := &Station{
		cfg:  cfg,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID))),
		temp: 35 + float64(cfg.ID),
	}
	s.regs[RegTargetCycle] = cfg.TargetCycleMs
	s.regs[RegState] = uint16(Idle)
	return s
}

// Run drives the state machine until ctx is cancelled.
func (s *Station) Run(ctx context.Context) {
	s.set(RegState, uint16(Running))
	for {
		s.mu.RLock()
		target := time.Duration(s.regs[RegTargetCycle]) * time.Millisecond
		inject := s.regs[RegFaultInject]
		s.mu.RUnlock()

		if inject == 1 || s.rng.Float64() < s.cfg.FaultProb {
			s.enterFault(uint16(100 + s.rng.Intn(5)))
		}
		if s.inFault() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			if time.Now().After(s.fault) {
				s.clearFault()
			}
			continue
		}

		jitter := 1 + (s.rng.Float64()*2-1)*s.cfg.Jitter
		cycle := time.Duration(float64(target) * jitter)
		select {
		case <-ctx.Done():
			return
		case <-time.After(cycle):
		}
		s.completeCycle(cycle)
	}
}

func (s *Station) completeCycle(cycle time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regs[RegCycleMs] = uint16(cycle.Milliseconds())
	if s.rng.Float64() < s.cfg.ScrapProb {
		s.regs[RegScrapCount]++
	} else {
		s.regs[RegPartCount]++
	}
	s.temp += (s.rng.Float64() - 0.45) * 0.5
	s.regs[RegTempC10] = uint16(s.temp * 10)
}

func (s *Station) enterFault(code uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.regs[RegState] == uint16(Faulted) {
		return
	}
	s.regs[RegState] = uint16(Faulted)
	s.regs[RegFaultCode] = code
	s.regs[RegFaultInject] = 0
	s.fault = time.Now().Add(s.cfg.FaultDuration)
}

func (s *Station) clearFault() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regs[RegState] = uint16(Running)
	s.regs[RegFaultCode] = 0
}

func (s *Station) inFault() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.regs[RegState] == uint16(Faulted)
}

func (s *Station) set(addr, v uint16) {
	s.mu.Lock()
	s.regs[addr] = v
	s.mu.Unlock()
}

// ReadHolding implements modbus.RegisterStore.
func (s *Station) ReadHolding(addr, qty uint16) ([]uint16, error) {
	if int(addr)+int(qty) > NumRegs {
		return nil, modbus.ErrIllegalAddress
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint16, qty)
	copy(out, s.regs[addr:addr+qty])
	return out, nil
}

// WriteHolding implements modbus.RegisterStore. Only RW registers accept writes.
func (s *Station) WriteHolding(addr uint16, vals []uint16) error {
	if int(addr)+len(vals) > NumRegs {
		return modbus.ErrIllegalAddress
	}
	for i := range vals {
		a := addr + uint16(i)
		if a != RegTargetCycle && a != RegFaultInject {
			return modbus.ErrIllegalAddress
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range vals {
		a := addr + uint16(i)
		if a == RegTargetCycle && v < 50 {
			v = 50
		}
		s.regs[a] = v
	}
	return nil
}
