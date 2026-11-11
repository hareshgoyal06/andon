// Package events defines the message types shared between gateway, processor and API.
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
	TypeRecipe      Type = "recipe_changed"
	TypeCmdResult   Type = "command_result" // State carries the command ID, Error the failure if any
)

// StationEvent is emitted by the gateway whenever a station's observed state changes.
type StationEvent struct {
	Type      Type      `json:"type"`
	StationID string    `json:"station_id"`
	Timestamp time.Time `json:"ts"`

	State         string  `json:"state,omitempty"`
	PrevState     string  `json:"prev_state,omitempty"`
	CycleMs       uint16  `json:"cycle_ms,omitempty"`
	TargetCycleMs uint16  `json:"target_cycle_ms,omitempty"`
	PartCount     uint16  `json:"part_count,omitempty"`
	ScrapCount    uint16  `json:"scrap_count,omitempty"`
	TempC         float64 `json:"temp_c,omitempty"`
	FaultCode     uint16  `json:"fault_code,omitempty"`
	Error         string  `json:"error,omitempty"`
}

// CommandType enumerates operator commands sent to stations.
type CommandType string

const (
	CmdSetTargetCycle CommandType = "set_target_cycle"
	CmdInjectFault    CommandType = "inject_fault"
)

// Command is published by the API and executed by the gateway over Modbus.
type Command struct {
	ID        string      `json:"id"`
	Type      CommandType `json:"type"`
	StationID string      `json:"station_id"`
	Value     uint16      `json:"value,omitempty"`
	Actor     string      `json:"actor,omitempty"`
	Timestamp time.Time   `json:"ts"`
}

// Severity of an alert.
type Severity string

const (
	SevInfo     Severity = "info"
	SevWarning  Severity = "warning"
	SevCritical Severity = "critical"
)

// Alert is produced by the processor.
type Alert struct {
	ID        string    `json:"id"`
	StationID string    `json:"station_id"`
	Severity  Severity  `json:"severity"`
	Kind      string    `json:"kind"` // fault, cycle_drift, offline, bottleneck, recovered
	Message   string    `json:"message"`
	Timestamp time.Time `json:"ts"`
}

// OEE is a rolling-window snapshot for one station.
type OEE struct {
	StationID    string    `json:"station_id"`
	Timestamp    time.Time `json:"ts"`
	WindowSec    int       `json:"window_sec"`
	Availability float64   `json:"availability"`
	Performance  float64   `json:"performance"`
	Quality      float64   `json:"quality"`
	OEE          float64   `json:"oee"`
	AvgCycleMs   float64   `json:"avg_cycle_ms"`
	Parts        int       `json:"parts"`
	Scrap        int       `json:"scrap"`
	Faults       int       `json:"faults"`
	IsBottleneck bool      `json:"is_bottleneck"`
}
