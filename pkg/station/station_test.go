package station

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hareshgoyal06/andon/pkg/modbus"
)

func startStation(t *testing.T, cfg Config) (*Station, *modbus.Client) {
	t.Helper()
	st := New(cfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go st.Run(ctx)
	go (&modbus.Server{Store: st}).Serve(ctx, ln)

	c := &modbus.Client{Addr: ln.Addr().String(), Unit: 1}
	if err := c.Dial(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return st, c
}

func waitFor(t *testing.T, c *modbus.Client, pred func(regs []uint16) bool, timeout time.Duration) []uint16 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		regs, err := c.ReadHoldingRegisters(0, 5)
		if err != nil {
			t.Fatal(err)
		}
		if pred(regs) {
			return regs
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
	return nil
}

func TestProducesParts(t *testing.T) {
	cfg := DefaultConfig(1)
	cfg.TargetCycleMs = 50
	cfg.FaultProb = 0
	_, c := startStation(t, cfg)

	regs := waitFor(t, c, func(r []uint16) bool { return r[RegPartCount] >= 3 }, 2*time.Second)
	if State(regs[RegState]) != Running {
		t.Fatalf("state = %v, want running", State(regs[RegState]))
	}
	if regs[RegCycleMs] == 0 {
		t.Fatal("cycle time not recorded")
	}
}

func TestFaultInjectionViaModbus(t *testing.T) {
	cfg := DefaultConfig(1)
	cfg.TargetCycleMs = 50
	cfg.FaultProb = 0
	cfg.FaultDuration = 200 * time.Millisecond
	_, c := startStation(t, cfg)

	// Chaos: write FAULT_INJECT=1 over the wire.
	if err := c.WriteSingleRegister(RegFaultInject, 1); err != nil {
		t.Fatal(err)
	}
	regs := waitFor(t, c, func(r []uint16) bool { return State(r[RegState]) == Faulted }, time.Second)
	if regs[RegFaultCode] == 0 {
		t.Fatal("fault code should be non-zero while faulted")
	}
	// Auto-recovers.
	waitFor(t, c, func(r []uint16) bool { return State(r[RegState]) == Running && r[RegFaultCode] == 0 }, 2*time.Second)
}

func TestRecipePushViaModbus(t *testing.T) {
	cfg := DefaultConfig(1)
	cfg.TargetCycleMs = 500
	cfg.FaultProb = 0
	cfg.Jitter = 0
	_, c := startStation(t, cfg)

	// Process change request: drop target cycle time to 60ms.
	if err := c.WriteSingleRegister(RegTargetCycle, 60); err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadHoldingRegisters(RegTargetCycle, 1)
	if err != nil || got[0] != 60 {
		t.Fatalf("target cycle = %v (%v), want 60", got, err)
	}
	// Station should now be cycling fast.
	waitFor(t, c, func(r []uint16) bool { return r[RegPartCount] >= 5 }, 2*time.Second)
}

func TestReadOnlyRegistersRejectWrites(t *testing.T) {
	_, c := startStation(t, DefaultConfig(1))
	err := c.WriteSingleRegister(RegPartCount, 999)
	var ex *modbus.Exception
	if !errors.As(err, &ex) || ex.Code != modbus.ExIllegalDataAddress {
		t.Fatalf("want illegal address exception, got %v", err)
	}
}
