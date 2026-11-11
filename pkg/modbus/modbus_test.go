package modbus

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

// memStore is a simple in-memory register bank for tests.
type memStore struct {
	mu   sync.Mutex
	regs [32]uint16
}

func (m *memStore) ReadHolding(addr, qty uint16) ([]uint16, error) {
	if int(addr)+int(qty) > len(m.regs) {
		return nil, ErrIllegalAddress
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]uint16, qty)
	copy(out, m.regs[addr:addr+qty])
	return out, nil
}

func (m *memStore) WriteHolding(addr uint16, vals []uint16) error {
	if int(addr)+len(vals) > len(m.regs) {
		return ErrIllegalAddress
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copy(m.regs[addr:], vals)
	return nil
}

func TestFrameRoundTrip(t *testing.T) {
	in := &Frame{TxID: 0xBEEF, Unit: 7, Func: FuncReadHoldingRegisters, Data: []byte{0, 10, 0, 2}}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, in); err != nil {
		t.Fatal(err)
	}
	// MBAP: txid(2) proto(2) len(2) unit(1) -> len must be unit+func+data = 6
	b := buf.Bytes()
	if got := int(b[4])<<8 | int(b[5]); got != 6 {
		t.Fatalf("length field = %d, want 6", got)
	}
	out, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.TxID != in.TxID || out.Unit != in.Unit || out.Func != in.Func || !bytes.Equal(out.Data, in.Data) {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
}

func startServer(t *testing.T, store RegisterStore) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := &Server{Store: store}
	go srv.Serve(ctx, ln)
	return ln.Addr().String()
}

func TestClientServer(t *testing.T) {
	store := &memStore{}
	addr := startServer(t, store)

	c := &Client{Addr: addr, Unit: 1}
	if err := c.Dial(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.WriteSingleRegister(3, 0x1234); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMultipleRegisters(4, []uint16{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadHoldingRegisters(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint16{0x1234, 1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("regs = %v, want %v", got, want)
		}
	}
}

func TestExceptions(t *testing.T) {
	addr := startServer(t, &memStore{})
	c := &Client{Addr: addr, Unit: 1}
	if err := c.Dial(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err := c.ReadHoldingRegisters(30, 10) // out of range
	var ex *Exception
	if !errors.As(err, &ex) || ex.Code != ExIllegalDataAddress {
		t.Fatalf("want illegal address exception, got %v", err)
	}

	_, err = c.ReadHoldingRegisters(0, 0) // qty 0 is invalid
	if !errors.As(err, &ex) || ex.Code != ExIllegalDataValue {
		t.Fatalf("want illegal data value exception, got %v", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	addr := startServer(t, &memStore{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &Client{Addr: addr, Unit: 1}
			if err := c.Dial(context.Background()); err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for j := 0; j < 50; j++ {
				if _, err := c.ReadHoldingRegisters(0, 8); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
