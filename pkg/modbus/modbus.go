// Package modbus implements a minimal Modbus TCP client and server.
//
// Modbus TCP frame (MBAP header + PDU):
//
//	+------------+-------------+--------+---------+----------+----------+
//	| TxID (2)   | Proto (2)=0 | Len(2) | Unit(1) | Func(1)  | Data (n) |
//	+------------+-------------+--------+---------+----------+----------+
//	|<--------------- MBAP (7 bytes) ------------>|<------ PDU -------->|
//
// Len counts everything after itself (Unit + Func + Data).
package modbus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Function codes.
const (
	FuncReadHoldingRegisters   = 0x03
	FuncWriteSingleRegister    = 0x06
	FuncWriteMultipleRegisters = 0x10
)

// Exception codes.
const (
	ExIllegalFunction    = 0x01
	ExIllegalDataAddress = 0x02
	ExIllegalDataValue   = 0x03
	ExServerFailure      = 0x04
)

const (
	mbapLen        = 7
	maxPDULen      = 253
	maxReadRegs    = 125
	maxWriteRegs   = 123
	protocolModbus = 0
)

// Frame is a decoded Modbus TCP ADU.
type Frame struct {
	TxID uint16
	Unit uint8
	Func uint8
	Data []byte
}

// Exception is returned by the client when the server replies with an exception PDU.
type Exception struct {
	Func uint8
	Code uint8
}

func (e *Exception) Error() string {
	return fmt.Sprintf("modbus exception: func=0x%02x code=0x%02x", e.Func, e.Code)
}

// ReadFrame reads one ADU from r.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [mbapLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	txID := binary.BigEndian.Uint16(hdr[0:2])
	proto := binary.BigEndian.Uint16(hdr[2:4])
	length := binary.BigEndian.Uint16(hdr[4:6])
	unit := hdr[6]

	if proto != protocolModbus {
		return nil, fmt.Errorf("modbus: bad protocol id %d", proto)
	}
	if length < 2 || length > maxPDULen+1 {
		return nil, fmt.Errorf("modbus: bad length %d", length)
	}
	// length includes unit (already read) + func + data
	rest := make([]byte, length-1)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, err
	}
	return &Frame{TxID: txID, Unit: unit, Func: rest[0], Data: rest[1:]}, nil
}

// WriteFrame encodes f and writes it to w.
func WriteFrame(w io.Writer, f *Frame) error {
	if len(f.Data)+1 > maxPDULen {
		return errors.New("modbus: pdu too large")
	}
	buf := make([]byte, mbapLen+1+len(f.Data))
	binary.BigEndian.PutUint16(buf[0:2], f.TxID)
	binary.BigEndian.PutUint16(buf[2:4], protocolModbus)
	binary.BigEndian.PutUint16(buf[4:6], uint16(2+len(f.Data))) // unit + func + data
	buf[6] = f.Unit
	buf[7] = f.Func
	copy(buf[8:], f.Data)
	_, err := w.Write(buf)
	return err
}

// exceptionFrame builds an exception response for req.
func exceptionFrame(req *Frame, code uint8) *Frame {
	return &Frame{TxID: req.TxID, Unit: req.Unit, Func: req.Func | 0x80, Data: []byte{code}}
}
