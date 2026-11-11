// Package oee implements per-station rolling-window OEE and anomaly detection.
//
// OEE = Availability × Performance × Quality
//
//	Availability = 1 − (time faulted or offline ÷ window)
//	Performance  = target cycle time ÷ observed mean cycle time (capped at 1)
//	Quality      = good parts ÷ (good parts + scrap)
package oee

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/hareshgoyal06/andon/pkg/events"
)

// Window is the rolling period OEE is computed over.
const Window = 5 * time.Minute

// cycleSample is one completed cycle.
type cycleSample struct {
	at      time.Time
	cycleMs float64
	scrap   bool
}

// downtime is a closed or open interval during which the station was not producing.
type downtime struct {
	from time.Time
	to   time.Time // zero if still ongoing
}

// Tracker holds state for one station.
type Tracker struct {
	ID string

	mu       sync.Mutex
	cycles   []cycleSample
	down     []downtime
	target   float64
	state    string
	online   bool
	lastSeen time.Time
	// cycle-time baseline for drift detection (Welford)
	n              int
	mean, m2       float64
	lastScrapCount uint16
	seenCounts     bool
}

// NewTracker creates an empty tracker.
func NewTracker(id string) *Tracker {
	return &Tracker{ID: id, online: true, state: "idle"}
}

// Apply updates the tracker with an event and returns any alerts it triggers.
func (t *Tracker) Apply(e events.StationEvent) []events.Alert {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSeen = e.Timestamp
	if e.TargetCycleMs > 0 {
		t.target = float64(e.TargetCycleMs)
	}
	var alerts []events.Alert

	switch e.Type {
	case events.TypeCycle:
		// The gateway emits one cycle event per counter increment; the cycle was scrap
		// if the scrap counter is what moved.
		scrap := t.seenCounts && e.ScrapCount != t.lastScrapCount
		t.lastScrapCount, t.seenCounts = e.ScrapCount, true
		t.cycles = append(t.cycles, cycleSample{at: e.Timestamp, cycleMs: float64(e.CycleMs), scrap: scrap})
		if a := t.checkDrift(e); a != nil {
			alerts = append(alerts, *a)
		}
		t.updateBaseline(float64(e.CycleMs))

	case events.TypeFault:
		t.state = "faulted"
		t.openDowntime(e.Timestamp)
		alerts = append(alerts, events.Alert{
			ID: alertID(t.ID, "fault", e.Timestamp), StationID: t.ID, Severity: events.SevCritical,
			Kind: "fault", Message: fmtFault(e.FaultCode), Timestamp: e.Timestamp,
		})

	case events.TypeFaultClear:
		t.state = e.State
		t.closeDowntime(e.Timestamp)
		alerts = append(alerts, events.Alert{
			ID: alertID(t.ID, "recovered", e.Timestamp), StationID: t.ID, Severity: events.SevInfo,
			Kind: "recovered", Message: "Fault cleared, station running", Timestamp: e.Timestamp,
		})

	case events.TypeOffline:
		t.online = false
		t.openDowntime(e.Timestamp)
		alerts = append(alerts, events.Alert{
			ID: alertID(t.ID, "offline", e.Timestamp), StationID: t.ID, Severity: events.SevCritical,
			Kind: "offline", Message: "Station unreachable: " + e.Error, Timestamp: e.Timestamp,
		})

	case events.TypeOnline:
		if !t.online {
			t.closeDowntime(e.Timestamp)
			alerts = append(alerts, events.Alert{
				ID: alertID(t.ID, "online", e.Timestamp), StationID: t.ID, Severity: events.SevInfo,
				Kind: "online", Message: "Station reconnected", Timestamp: e.Timestamp,
			})
		}
		t.online = true

	case events.TypeStateChange:
		if e.State != "" {
			t.state = e.State
		}
		if e.State == "faulted" {
			t.openDowntime(e.Timestamp)
		} else if e.PrevState == "faulted" {
			t.closeDowntime(e.Timestamp)
		}

	case events.TypeRecipe:
		// Reset drift baseline: a new recipe legitimately changes cycle time.
		t.n, t.mean, t.m2 = 0, 0, 0
		alerts = append(alerts, events.Alert{
			ID: alertID(t.ID, "recipe", e.Timestamp), StationID: t.ID, Severity: events.SevInfo,
			Kind: "recipe", Message: "Target cycle time changed to " + itoa(int(e.TargetCycleMs)) + " ms", Timestamp: e.Timestamp,
		})
	}

	t.prune(e.Timestamp)
	return alerts
}

// lastScrapCount is stored on the tracker (declared here to keep struct above readable).
func (t *Tracker) openDowntime(at time.Time) {
	if n := len(t.down); n > 0 && t.down[n-1].to.IsZero() {
		return
	}
	t.down = append(t.down, downtime{from: at})
}

func (t *Tracker) closeDowntime(at time.Time) {
	if n := len(t.down); n > 0 && t.down[n-1].to.IsZero() {
		t.down[n-1].to = at
	}
}

func (t *Tracker) prune(now time.Time) {
	cut := now.Add(-Window)
	i := sort.Search(len(t.cycles), func(i int) bool { return t.cycles[i].at.After(cut) })
	t.cycles = t.cycles[i:]
	j := 0
	for _, d := range t.down {
		if d.to.IsZero() || d.to.After(cut) {
			t.down[j] = d
			j++
		}
	}
	t.down = t.down[:j]
}

func (t *Tracker) updateBaseline(x float64) {
	t.n++
	delta := x - t.mean
	t.mean += delta / float64(t.n)
	t.m2 += delta * (x - t.mean)
}

// checkDrift flags a cycle more than 2σ above the running mean once we have a baseline.
func (t *Tracker) checkDrift(e events.StationEvent) *events.Alert {
	if t.n < 20 {
		return nil
	}
	sd := math.Sqrt(t.m2 / float64(t.n-1))
	if sd == 0 {
		return nil
	}
	x := float64(e.CycleMs)
	if z := (x - t.mean) / sd; z > 2 {
		return &events.Alert{
			ID: alertID(t.ID, "drift", e.Timestamp), StationID: t.ID, Severity: events.SevWarning,
			Kind: "cycle_drift", Timestamp: e.Timestamp,
			Message: "Cycle time " + itoa(int(x)) + " ms is " + ftoa(z) + "σ above baseline " + itoa(int(t.mean)) + " ms",
		}
	}
	return nil
}

// Snapshot computes OEE as of now.
func (t *Tracker) Snapshot(now time.Time) events.OEE {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prune(now)

	o := events.OEE{StationID: t.ID, Timestamp: now, WindowSec: int(Window.Seconds())}
	start := now.Add(-Window)

	// Availability
	var down time.Duration
	for _, d := range t.down {
		from := d.from
		if from.Before(start) {
			from = start
		}
		to := d.to
		if to.IsZero() {
			to = now
		}
		if to.After(from) {
			down += to.Sub(from)
		}
	}
	o.Availability = clamp(1 - down.Seconds()/Window.Seconds())

	// Performance & Quality
	var sum float64
	for _, c := range t.cycles {
		sum += c.cycleMs
		if c.scrap {
			o.Scrap++
		} else {
			o.Parts++
		}
	}
	if n := len(t.cycles); n > 0 {
		o.AvgCycleMs = sum / float64(n)
		if t.target > 0 && o.AvgCycleMs > 0 {
			o.Performance = clamp(t.target / o.AvgCycleMs)
		}
	}
	if total := o.Parts + o.Scrap; total > 0 {
		o.Quality = float64(o.Parts) / float64(total)
	} else {
		o.Quality = 1
	}
	o.Faults = len(t.down)
	o.OEE = o.Availability * o.Performance * o.Quality
	return o
}

// MarkBottleneck finds the slowest station by average cycle time if it is more
// than 15% slower than the line median, and flags it in the slice.
func MarkBottleneck(snaps []events.OEE) (string, bool) {
	if len(snaps) < 3 {
		return "", false
	}
	avgs := make([]float64, 0, len(snaps))
	for _, s := range snaps {
		if s.AvgCycleMs > 0 {
			avgs = append(avgs, s.AvgCycleMs)
		}
	}
	if len(avgs) < 3 {
		return "", false
	}
	sort.Float64s(avgs)
	median := avgs[len(avgs)/2]
	worstIdx, worst := -1, 0.0
	for i, s := range snaps {
		if s.AvgCycleMs > worst {
			worst, worstIdx = s.AvgCycleMs, i
		}
	}
	if worstIdx >= 0 && worst > median*1.15 {
		snaps[worstIdx].IsBottleneck = true
		return snaps[worstIdx].StationID, true
	}
	return "", false
}

func clamp(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func alertID(station, kind string, at time.Time) string {
	return station + ":" + kind + ":" + at.UTC().Format(time.RFC3339Nano)
}

func fmtFault(code uint16) string {
	names := map[uint16]string{100: "E-stop pressed", 101: "Servo overload", 102: "Vision reject threshold", 103: "Fixture clamp timeout", 104: "Torque out of spec"}
	if n, ok := names[code]; ok {
		return "Fault " + itoa(int(code)) + ": " + n
	}
	return "Fault " + itoa(int(code))
}

func itoa(i int) string { return formatInt(int64(i)) }
func ftoa(f float64) string {
	return formatInt(int64(f*10)/10) + "." + formatInt(int64(math.Abs(f)*10)%10)
}
func formatInt(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
