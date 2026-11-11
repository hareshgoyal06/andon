package modbus

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"sync"
)

// RegisterStore is the backing store the server reads from and writes to.
// Implementations must be safe for concurrent use.
type RegisterStore interface {
	// ReadHolding returns qty registers starting at addr, or an error if out of range.
	ReadHolding(addr, qty uint16) ([]uint16, error)
	// WriteHolding writes vals starting at addr, or returns an error if out of range / read-only.
	WriteHolding(addr uint16, vals []uint16) error
}

// ErrIllegalAddress should be returned by stores for out-of-range or read-only addresses.
var ErrIllegalAddress = errors.New("modbus: illegal data address")

// Server is a Modbus TCP server backed by a RegisterStore.
type Server struct {
	Store RegisterStore
	Log   *slog.Logger

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// Serve accepts connections on ln until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	s.conns = make(map[net.Conn]struct{})

	go func() {
		<-ctx.Done()
		ln.Close()
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		s.track(conn, true)
		go func() {
			defer s.track(conn, false)
			defer conn.Close()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) track(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	for {
		req, err := ReadFrame(conn)
		if err != nil {
			return // EOF or malformed; drop the connection
		}
		resp := s.handle(req)
		if err := WriteFrame(conn, resp); err != nil {
			return
		}
	}
}

func (s *Server) handle(req *Frame) *Frame {
	switch req.Func {
	case FuncReadHoldingRegisters:
		if len(req.Data) != 4 {
			return exceptionFrame(req, ExIllegalDataValue)
		}
		addr := binary.BigEndian.Uint16(req.Data[0:2])
		qty := binary.BigEndian.Uint16(req.Data[2:4])
		if qty == 0 || qty > maxReadRegs {
			return exceptionFrame(req, ExIllegalDataValue)
		}
		vals, err := s.Store.ReadHolding(addr, qty)
		if err != nil {
			return exceptionFrame(req, ExIllegalDataAddress)
		}
		data := make([]byte, 1+2*len(vals))
		data[0] = byte(2 * len(vals))
		for i, v := range vals {
			binary.BigEndian.PutUint16(data[1+2*i:], v)
		}
		return &Frame{TxID: req.TxID, Unit: req.Unit, Func: req.Func, Data: data}

	case FuncWriteSingleRegister:
		if len(req.Data) != 4 {
			return exceptionFrame(req, ExIllegalDataValue)
		}
		addr := binary.BigEndian.Uint16(req.Data[0:2])
		val := binary.BigEndian.Uint16(req.Data[2:4])
		if err := s.Store.WriteHolding(addr, []uint16{val}); err != nil {
			return exceptionFrame(req, ExIllegalDataAddress)
		}
		// Echo request on success.
		return &Frame{TxID: req.TxID, Unit: req.Unit, Func: req.Func, Data: req.Data}

	case FuncWriteMultipleRegisters:
		if len(req.Data) < 5 {
			return exceptionFrame(req, ExIllegalDataValue)
		}
		addr := binary.BigEndian.Uint16(req.Data[0:2])
		qty := binary.BigEndian.Uint16(req.Data[2:4])
		byteCount := int(req.Data[4])
		if qty == 0 || qty > maxWriteRegs || byteCount != 2*int(qty) || len(req.Data) != 5+byteCount {
			return exceptionFrame(req, ExIllegalDataValue)
		}
		vals := make([]uint16, qty)
		for i := range vals {
			vals[i] = binary.BigEndian.Uint16(req.Data[5+2*i:])
		}
		if err := s.Store.WriteHolding(addr, vals); err != nil {
			return exceptionFrame(req, ExIllegalDataAddress)
		}
		return &Frame{TxID: req.TxID, Unit: req.Unit, Func: req.Func, Data: req.Data[0:4]}

	default:
		return exceptionFrame(req, ExIllegalFunction)
	}
}
