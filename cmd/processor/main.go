// processor consumes station.events from Kafka, persists them idempotently to
// TimescaleDB, maintains per-station OEE trackers, detects faults/drift/
// bottlenecks, and publishes alerts and OEE snapshots.
//
// It is horizontally scalable: run N replicas in the same consumer group and
// partitions (keyed by station) are spread across them. On start, each replica
// rebuilds its in-memory window from the database so a failover loses nothing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/hareshgoyal06/andon/pkg/db"
	"github.com/hareshgoyal06/andon/pkg/events"
	"github.com/hareshgoyal06/andon/pkg/kafkax"
	"github.com/hareshgoyal06/andon/pkg/oee"
)

var (
	processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "andon_processor_events_total", Help: "Events processed by type",
	}, []string{"type"})
	alertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "andon_processor_alerts_total", Help: "Alerts raised by kind",
	}, []string{"kind", "severity"})
	oeeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "andon_station_oee", Help: "Rolling OEE per station (0-1)",
	}, []string{"station", "component"})
	cycleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "andon_station_avg_cycle_ms", Help: "Rolling average cycle time",
	}, []string{"station"})
	processLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "andon_processor_lag_seconds", Help: "Event timestamp to processing time",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type processor struct {
	log      *slog.Logger
	db       *db.DB
	alerts   *kafkax.Producer
	oeeOut   *kafkax.Producer
	mu       sync.Mutex
	trackers map[string]*oee.Tracker
	lastBN   string // last bottleneck station, to avoid re-alerting
	hostname string
}

func (p *processor) tracker(id string) *oee.Tracker {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.trackers[id]
	if !ok {
		t = oee.NewTracker(id)
		p.trackers[id] = t
	}
	return t
}

// rebuild warms trackers from the last Window of persisted events.
func (p *processor) rebuild(ctx context.Context) error {
	evs, err := p.db.RecentEvents(ctx, "", time.Now().Add(-oee.Window), 100000)
	if err != nil {
		return err
	}
	// RecentEvents is newest-first; replay oldest-first.
	for i := len(evs) - 1; i >= 0; i-- {
		p.tracker(evs[i].StationID).Apply(evs[i]) // discard alerts: already persisted
	}
	p.log.Info("rebuilt state", "events", len(evs), "stations", len(p.trackers))
	return nil
}

func (p *processor) handle(ctx context.Context, m kafka.Message) error {
	var e events.StationEvent
	if err := json.Unmarshal(m.Value, &e); err != nil {
		p.log.Warn("bad event payload", "err", err)
		return nil // skip poison pill
	}
	processed.WithLabelValues(string(e.Type)).Inc()
	processLag.Observe(time.Since(e.Timestamp).Seconds())

	if e.Type == events.TypeCmdResult {
		status, errMsg := "applied", ""
		if e.Error != "" {
			status, errMsg = "failed", e.Error
		}
		return p.db.MarkCommand(ctx, e.State, status, errMsg)
	}

	// Persist first (idempotent), then update state. If we crash between the
	// two, replay on restart is harmless because of ON CONFLICT DO NOTHING.
	if err := p.db.InsertEvent(ctx, e); err != nil {
		return err
	}

	t := p.tracker(e.StationID)
	alerts := t.Apply(e)

	online := e.Type != events.TypeOffline
	if err := p.db.UpsertStationState(ctx, db.StationState{
		StationID: e.StationID, State: e.State, Online: online, CycleMs: int(e.CycleMs),
		TargetCycleMs: int(e.TargetCycleMs), PartCount: int(e.PartCount), ScrapCount: int(e.ScrapCount),
		TempC: e.TempC, FaultCode: int(e.FaultCode), UpdatedAt: e.Timestamp,
	}); err != nil {
		return err
	}

	for _, a := range alerts {
		if err := p.emitAlert(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) emitAlert(ctx context.Context, a events.Alert) error {
	alertsTotal.WithLabelValues(a.Kind, string(a.Severity)).Inc()
	if err := p.db.InsertAlert(ctx, a); err != nil {
		return err
	}
	return p.alerts.Publish(ctx, a.StationID, a)
}

// snapshotLoop publishes OEE every interval and runs bottleneck detection.
func (p *processor) snapshotLoop(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		var now time.Time
		select {
		case <-ctx.Done():
			return
		case now = <-tick.C:
		}
		p.mu.Lock()
		snaps := make([]events.OEE, 0, len(p.trackers))
		for _, t := range p.trackers {
			snaps = append(snaps, t.Snapshot(now))
		}
		p.mu.Unlock()

		bn, isBN := oee.MarkBottleneck(snaps)
		if isBN && bn != p.lastBN {
			_ = p.emitAlert(ctx, events.Alert{
				ID: bn + ":bottleneck:" + now.UTC().Format(time.RFC3339), StationID: bn,
				Severity: events.SevWarning, Kind: "bottleneck", Timestamp: now,
				Message: "Station is the line bottleneck (slowest by >15% vs median)",
			})
		}
		if !isBN || bn != p.lastBN {
			p.lastBN = bn
		}

		for _, s := range snaps {
			oeeGauge.WithLabelValues(s.StationID, "oee").Set(s.OEE)
			oeeGauge.WithLabelValues(s.StationID, "availability").Set(s.Availability)
			oeeGauge.WithLabelValues(s.StationID, "performance").Set(s.Performance)
			oeeGauge.WithLabelValues(s.StationID, "quality").Set(s.Quality)
			cycleGauge.WithLabelValues(s.StationID).Set(s.AvgCycleMs)
			if err := p.db.InsertOEE(ctx, s); err != nil && ctx.Err() == nil {
				p.log.Error("insert oee", "err", err)
			}
			if err := p.oeeOut.Publish(ctx, s.StationID, s); err != nil && ctx.Err() == nil {
				p.log.Error("publish oee", "err", err)
			}
		}
	}
}

func main() {
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka brokers")
	dsn := flag.String("dsn", env("DATABASE_URL", "postgres://andon:andon@localhost:5432/andon"), "Postgres DSN")
	group := flag.String("group", env("CONSUMER_GROUP", "andon-processor"), "consumer group")
	metricsAddr := flag.String("metrics-addr", env("METRICS_ADDR", ":9102"), "metrics/health address")
	snapshotEvery := flag.Duration("snapshot-every", 5*time.Second, "OEE snapshot interval")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bl := kafkax.Brokers(*brokers)
	if err := kafkax.EnsureTopics(ctx, log, bl, 6, kafkax.TopicEvents, kafkax.TopicAlerts, kafkax.TopicOEE); err != nil {
		log.Error("ensure topics", "err", err)
		os.Exit(1)
	}
	database, err := db.Connect(ctx, log, *dsn)
	if err != nil {
		log.Error("db", "err", err)
		os.Exit(1)
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	host, _ := os.Hostname()
	p := &processor{
		log: log, db: database, trackers: map[string]*oee.Tracker{}, hostname: host,
		alerts: kafkax.NewProducer(bl, kafkax.TopicAlerts),
		oeeOut: kafkax.NewProducer(bl, kafkax.TopicOEE),
	}
	defer p.alerts.Close()
	defer p.oeeOut.Close()

	if err := p.rebuild(ctx); err != nil {
		log.Error("rebuild", "err", err)
		os.Exit(1)
	}

	ready := false
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(503)
		}
	})
	hs := &http.Server{Addr: *metricsAddr, Handler: mux}
	go func() { _ = hs.ListenAndServe() }()

	go p.snapshotLoop(ctx, *snapshotEvery)

	r := kafkax.NewReader(kafkax.ReaderConfig{Brokers: bl, Topic: kafkax.TopicEvents, GroupID: *group})
	defer r.Close()
	ready = true
	log.Info("processor started", "host", host, "group", *group)
	if err := kafkax.Consume(ctx, r, p.handle); err != nil {
		log.Error("consume", "err", err)
		os.Exit(1)
	}
	_ = hs.Close()
	log.Info("shutdown complete")
}
