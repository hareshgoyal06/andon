package oee

import (
	"testing"
	"time"

	"github.com/hareshgoyal06/andon/pkg/events"
)

func cycle(id string, at time.Time, ms, target, parts, scrap uint16) events.StationEvent {
	return events.StationEvent{Type: events.TypeCycle, StationID: id, Timestamp: at, CycleMs: ms,
		TargetCycleMs: target, PartCount: parts, ScrapCount: scrap, State: "running"}
}

func TestPerfectStation(t *testing.T) {
	tr := NewTracker("s1")
	start := time.Now().Add(-time.Minute)
	for i := 0; i < 30; i++ {
		tr.Apply(cycle("s1", start.Add(time.Duration(i)*time.Second), 1000, 1000, uint16(i+1), 0))
	}
	o := tr.Snapshot(start.Add(31 * time.Second))
	if o.Availability != 1 || o.Performance != 1 || o.Quality != 1 || o.OEE != 1 {
		t.Fatalf("expected perfect OEE, got %+v", o)
	}
	if o.Parts != 30 {
		t.Fatalf("parts = %d, want 30", o.Parts)
	}
}

func TestAvailabilityFromFault(t *testing.T) {
	tr := NewTracker("s1")
	now := time.Now()
	faultAt := now.Add(-60 * time.Second)
	tr.Apply(events.StationEvent{Type: events.TypeFault, StationID: "s1", Timestamp: faultAt, FaultCode: 101})
	tr.Apply(events.StationEvent{Type: events.TypeFaultClear, StationID: "s1", Timestamp: faultAt.Add(30 * time.Second), State: "running"})
	o := tr.Snapshot(now)
	want := 1 - 30.0/Window.Seconds()
	if d := o.Availability - want; d > 0.001 || d < -0.001 {
		t.Fatalf("availability = %.4f, want %.4f", o.Availability, want)
	}
	if o.Faults != 1 {
		t.Fatalf("faults = %d, want 1", o.Faults)
	}
}

func TestOngoingFaultCountsUntilNow(t *testing.T) {
	tr := NewTracker("s1")
	now := time.Now()
	tr.Apply(events.StationEvent{Type: events.TypeFault, StationID: "s1", Timestamp: now.Add(-Window / 2)})
	o := tr.Snapshot(now)
	if o.Availability > 0.51 || o.Availability < 0.49 {
		t.Fatalf("availability = %.3f, want ~0.5", o.Availability)
	}
}

func TestQualityAndPerformance(t *testing.T) {
	tr := NewTracker("s1")
	start := time.Now().Add(-time.Minute)
	parts, scrap := uint16(0), uint16(0)
	for i := 0; i < 10; i++ {
		if i%5 == 4 {
			scrap++
		} else {
			parts++
		}
		// actual 1250ms vs target 1000ms -> performance 0.8
		tr.Apply(cycle("s1", start.Add(time.Duration(i)*time.Second), 1250, 1000, parts, scrap))
	}
	o := tr.Snapshot(start.Add(11 * time.Second))
	// First event cannot be classified (no baseline), so 9 classified: 7 good + 2 scrap.
	if o.Scrap != 2 {
		t.Fatalf("scrap = %d, want 2 (got %+v)", o.Scrap, o)
	}
	if d := o.Performance - 0.8; d > 0.001 || d < -0.001 {
		t.Fatalf("performance = %.3f, want 0.8", o.Performance)
	}
	if o.Quality <= 0.7 || o.Quality >= 0.9 {
		t.Fatalf("quality = %.3f, want ~0.8", o.Quality)
	}
}

func TestDriftAlert(t *testing.T) {
	tr := NewTracker("s1")
	start := time.Now().Add(-time.Minute)
	var alerts []events.Alert
	for i := 0; i < 25; i++ {
		ms := uint16(1000 + (i%3)*10) // tight baseline ~1010 ± 8
		alerts = append(alerts, tr.Apply(cycle("s1", start.Add(time.Duration(i)*time.Second), ms, 1000, uint16(i), 0))...)
	}
	if len(alerts) != 0 {
		t.Fatalf("unexpected alerts during baseline: %+v", alerts)
	}
	alerts = tr.Apply(cycle("s1", start.Add(30*time.Second), 1400, 1000, 30, 0))
	if len(alerts) != 1 || alerts[0].Kind != "cycle_drift" {
		t.Fatalf("want drift alert, got %+v", alerts)
	}
}

func TestRecipeChangeResetsBaseline(t *testing.T) {
	tr := NewTracker("s1")
	start := time.Now().Add(-time.Minute)
	for i := 0; i < 25; i++ {
		tr.Apply(cycle("s1", start.Add(time.Duration(i)*time.Second), 1000, 1000, uint16(i), 0))
	}
	a := tr.Apply(events.StationEvent{Type: events.TypeRecipe, StationID: "s1", Timestamp: start.Add(26 * time.Second), TargetCycleMs: 2000})
	if len(a) != 1 || a[0].Kind != "recipe" {
		t.Fatalf("want recipe alert, got %+v", a)
	}
	// A 2000ms cycle would have been a drift alert before the reset.
	a = tr.Apply(cycle("s1", start.Add(27*time.Second), 2000, 2000, 26, 0))
	if len(a) != 0 {
		t.Fatalf("baseline should have reset, got %+v", a)
	}
}

func TestWindowPrunes(t *testing.T) {
	tr := NewTracker("s1")
	old := time.Now().Add(-2 * Window)
	for i := 0; i < 10; i++ {
		tr.Apply(cycle("s1", old.Add(time.Duration(i)*time.Second), 1000, 1000, uint16(i), 0))
	}
	o := tr.Snapshot(time.Now())
	if o.Parts != 0 || o.AvgCycleMs != 0 {
		t.Fatalf("old cycles should be pruned, got %+v", o)
	}
}

func TestBottleneck(t *testing.T) {
	snaps := []events.OEE{
		{StationID: "a", AvgCycleMs: 1000},
		{StationID: "b", AvgCycleMs: 1020},
		{StationID: "c", AvgCycleMs: 1500},
		{StationID: "d", AvgCycleMs: 990},
	}
	id, ok := MarkBottleneck(snaps)
	if !ok || id != "c" || !snaps[2].IsBottleneck {
		t.Fatalf("want c as bottleneck, got %q %v", id, ok)
	}
	even := []events.OEE{{StationID: "a", AvgCycleMs: 1000}, {StationID: "b", AvgCycleMs: 1050}, {StationID: "c", AvgCycleMs: 1100}}
	if _, ok := MarkBottleneck(even); ok {
		t.Fatal("balanced line should have no bottleneck")
	}
}
