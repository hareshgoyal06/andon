// Package events defines the event types shared between gateway, processor and API.
package events

import "time"

// Type of a station event.
type Type string

const (
	TypeStateChange Type = "state_change"
	TypeCycle       Type = "cycle_complete"
	TypeFault       Type = "fault"
	TypeFaultClear  Type = "fault_cleared"
	TypeOffline     Type = "offline"
	TypeOnline      Type = "online"
)

// StationEvent is emitted by the gateway whenever a station's observed state changes.
type StationEvent struct {
	Type      Type      `json:"type"`
	StationID string    `json:"station_id"`
	Timestamp time.Time `json:"ts"`

	State     string  `json:"state,omitempty"`
	PrevState string  `json:"prev_state,omitempty"`
	CycleMs   uint16  `json:"cycle_ms,omitempty"`
	PartCount uint16  `json:"part_count,omitempty"`
	TempC     float64 `json:"temp_c,omitempty"`
	FaultCode uint16  `json:"fault_code,omitempty"`
	Error     string  `json:"error,omitempty"`
}
