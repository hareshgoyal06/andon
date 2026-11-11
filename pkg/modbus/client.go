package modbus

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client is a Modbus TCP client. It is safe for concurrent use; requests are
// serialised over the single connection (Modbus TCP servers commonly process
// one request at a time per connection).
type Client struct {
	Addr    string
	Unit    uint8
	Timeout time.Duration

	mu   sync.Mutex
	conn net.Conn
	txID uint16
}

// Dial opens the connection. Call Close when done.
func (c *Client) Dial(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Second
	}
	d := net.Dialer{Timeout: c.Timeout}
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) roundTrip(fn uint8, data []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, fmt.Errorf("modbus: not connected")
	}
	c.txID++
	req := &Frame{TxID: c.txID, Unit: c.Unit, Func: fn, Data: data}

	c.conn.SetDeadline(time.Now().Add(c.Timeout))
	if err := WriteFrame(c.conn, req); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, err
	}
	resp, err := ReadFrame(c.conn)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, err
	}
	if resp.TxID != req.TxID {
		return nil, fmt.Errorf("modbus: txid mismatch want %d got %d", req.TxID, resp.TxID)
	}
	if resp.Func&0x80 != 0 {
		code := uint8(0)
		if len(resp.Data) > 0 {
			code = resp.Data[0]
		}
		return nil, &Exception{Func: fn, Code: code}
	}
	if resp.Func != fn {
		return nil, fmt.Errorf("modbus: func mismatch want 0x%02x got 0x%02x", fn, resp.Func)
	}
	return resp.Data, nil
}

// ReadHoldingRegisters reads qty registers starting at addr (function 0x03).
func (c *Client) ReadHoldingRegisters(addr, qty uint16) ([]uint16, error) {
	var data [4]byte
	binary.BigEndian.PutUint16(data[0:2], addr)
	binary.BigEndian.PutUint16(data[2:4], qty)
	resp, err := c.roundTrip(FuncReadHoldingRegisters, data[:])
	if err != nil {
		return nil, err
	}
	if len(resp) < 1 || int(resp[0]) != len(resp)-1 || resp[0] != byte(2*qty) {
		return nil, fmt.Errorf("modbus: malformed read response")
	}
	out := make([]uint16, qty)
	for i := range out {
		out[i] = binary.BigEndian.Uint16(resp[1+2*i:])
	}
	return out, nil
}

// WriteSingleRegister writes one register (function 0x06).
func (c *Client) WriteSingleRegister(addr, val uint16) error {
	var data [4]byte
	binary.BigEndian.PutUint16(data[0:2], addr)
	binary.BigEndian.PutUint16(data[2:4], val)
	_, err := c.roundTrip(FuncWriteSingleRegister, data[:])
	return err
}

// WriteMultipleRegisters writes vals starting at addr (function 0x10).
func (c *Client) WriteMultipleRegisters(addr uint16, vals []uint16) error {
	data := make([]byte, 5+2*len(vals))
	binary.BigEndian.PutUint16(data[0:2], addr)
	binary.BigEndian.PutUint16(data[2:4], uint16(len(vals)))
	data[4] = byte(2 * len(vals))
	for i, v := range vals {
		binary.BigEndian.PutUint16(data[5+2*i:], v)
	}
	_, err := c.roundTrip(FuncWriteMultipleRegisters, data)
	return err
}
