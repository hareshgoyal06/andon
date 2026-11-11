// gateway polls stations over Modbus TCP, diffs register snapshots, and
// publishes StationEvents to Kafka. It also consumes operator Commands from
// Kafka and executes them against the PLCs (recipe changes, fault injection).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/hareshgoyal06/andon/pkg/events"
	"github.com/hareshgoyal06/andon/pkg/kafkax"
	"github.com/hareshgoyal06/andon/pkg/modbus"
	"github.com/hareshgoyal06/andon/pkg/station"
)

var (
	pollLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "andon_gateway_poll_seconds", Help: "Modbus poll round-trip latency",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	}, []string{"station"})
	eventsEmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "andon_gateway_events_total", Help: "Events emitted by type",
	}, []string{"type"})
	stationOnline = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "andon_gateway_station_online", Help: "1 if station reachable",
	}, []string{"station"})
	commandsHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "andon_gateway_commands_total", Help: "Commands executed by result",
	}, []string{"result"})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Sink receives events.
type Sink interface {
	Emit(ctx context.Context, ev events.StationEvent)
}

type stdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (s *stdoutSink) Emit(_ context.Context, ev events.StationEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(ev)
}

type kafkaSink struct {
	p   *kafkax.Producer
	log *slog.Logger
}

func (k *kafkaSink) Emit(ctx context.Context, ev events.StationEvent) {
	eventsEmitted.WithLabelValues(string(ev.Type)).Inc()
	if err := k.p.Publish(ctx, ev.StationID, ev); err != nil && ctx.Err() == nil {
		k.log.Error("publish", "err", err, "station", ev.StationID)
	}
}

// target is a station the gateway polls.
type target struct {
	id   string
	addr string
}

func parseTargets(s string) []target {
	var out []target
	for i, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		t := target{id: fmt.Sprintf("station-%d", i+1), addr: part}
		if id, addr, ok := strings.Cut(part, "="); ok {
			t.id, t.addr = id, addr
		}
		out = append(out, t)
	}
	return out
}

func main() {
	stationsFlag := flag.String("stations", env("STATIONS", "localhost:5020"), "comma-separated [id=]host:port list")
	interval := flag.Duration("interval", 100*time.Millisecond, "poll interval")
	brokers := flag.String("brokers", env("KAFKA_BROKERS", ""), "Kafka brokers; empty = print to stdout")
	metricsAddr := flag.String("metrics-addr", env("METRICS_ADDR", ":9101"), "metrics/health listen address")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	targets := parseTargets(*stationsFlag)
	if len(targets) == 0 {
		log.Error("no stations configured")
		os.Exit(1)
	}

	// Shared Modbus clients so the command executor can write to the same connections.
	clients := make(map[string]*modbus.Client, len(targets))
	var clientsMu sync.RWMutex

	var sink Sink = &stdoutSink{enc: json.NewEncoder(os.Stdout)}
	var wg sync.WaitGroup

	if *brokers != "" {
		bl := kafkax.Brokers(*brokers)
		if err := kafkax.EnsureTopics(ctx, log, bl, 6, kafkax.TopicEvents, kafkax.TopicCommands, kafkax.TopicAlerts, kafkax.TopicOEE); err != nil {
			log.Error("ensure topics", "err", err)
			os.Exit(1)
		}
		p := kafkax.NewProducer(bl, kafkax.TopicEvents)
		defer p.Close()
		sink = &kafkaSink{p: p, log: log}

		// Command consumer: API -> Kafka -> Modbus write.
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := kafkax.NewReader(kafkax.ReaderConfig{Brokers: bl, Topic: kafkax.TopicCommands, GroupID: "andon-gateway"})
			defer r.Close()
			err := kafkax.Consume(ctx, r, func(ctx context.Context, m kafka.Message) error {
				var cmd events.Command
				if err := json.Unmarshal(m.Value, &cmd); err != nil {
					log.Warn("bad command payload", "err", err)
					return nil // poison pill: skip
				}
				clientsMu.RLock()
				cli := clients[cmd.StationID]
				clientsMu.RUnlock()
				res := executeCommand(cli, cmd)
				if res != nil {
					commandsHandled.WithLabelValues("error").Inc()
					log.Warn("command failed", "id", cmd.ID, "station", cmd.StationID, "err", res)
				} else {
					commandsHandled.WithLabelValues("ok").Inc()
					log.Info("command applied", "id", cmd.ID, "type", cmd.Type, "station", cmd.StationID, "value", cmd.Value)
				}
				// Report outcome back as an event so the API can mark the command.
				sink.Emit(ctx, events.StationEvent{
					Type: "command_result", StationID: cmd.StationID, Timestamp: time.Now(),
					Error: errString(res), State: cmd.ID,
				})
				return nil
			})
			if err != nil && ctx.Err() == nil {
				log.Error("command consumer", "err", err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	hs := &http.Server{Addr: *metricsAddr, Handler: mux}
	go func() { _ = hs.ListenAndServe() }()

	for _, t := range targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			pollStation(ctx, log, sink, t, *interval, func(c *modbus.Client) {
				clientsMu.Lock()
				if c == nil {
					delete(clients, t.id)
				} else {
					clients[t.id] = c
				}
				clientsMu.Unlock()
			})
		}(t)
	}

	<-ctx.Done()
	_ = hs.Close()
	wg.Wait()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func executeCommand(cli *modbus.Client, cmd events.Command) error {
	if cli == nil {
		return fmt.Errorf("station %s not connected", cmd.StationID)
	}
	switch cmd.Type {
	case events.CmdSetTargetCycle:
		return cli.WriteSingleRegister(station.RegTargetCycle, cmd.Value)
	case events.CmdInjectFault:
		return cli.WriteSingleRegister(station.RegFaultInject, 1)
	default:
		return fmt.Errorf("unknown command type %q", cmd.Type)
	}
}

// snapshot is the subset of registers we track for diffing.
type snapshot struct {
	state      station.State
	cycleMs    uint16
	partCount  uint16
	tempC10    uint16
	faultCode  uint16
	scrapCount uint16
	target     uint16
}

func pollStation(ctx context.Context, log *slog.Logger, sink Sink, t target, interval time.Duration, register func(*modbus.Client)) {
	var (
		cli    *modbus.Client
		prev   *snapshot
		online bool
	)
	backoff := time.Second
	tick := time.NewTicker(interval)
	defer tick.Stop()
	stationOnline.WithLabelValues(t.id).Set(0)

	for {
		select {
		case <-ctx.Done():
			if cli != nil {
				cli.Close()
			}
			return
		case <-tick.C:
		}

		if cli == nil {
			cli = &modbus.Client{Addr: t.addr, Unit: 1}
			if err := cli.Dial(ctx); err != nil {
				cli = nil
				if online {
					online = false
					stationOnline.WithLabelValues(t.id).Set(0)
					sink.Emit(ctx, events.StationEvent{Type: events.TypeOffline, StationID: t.id, Timestamp: time.Now(), Error: err.Error()})
				}
				log.Warn("dial failed", "station", t.id, "err", err, "retry_in", backoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 10*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second
			register(cli)
			if !online {
				online = true
				stationOnline.WithLabelValues(t.id).Set(1)
				sink.Emit(ctx, events.StationEvent{Type: events.TypeOnline, StationID: t.id, Timestamp: time.Now()})
			}
		}

		start := time.Now()
		regs, err := cli.ReadHoldingRegisters(0, 12)
		pollLatency.WithLabelValues(t.id).Observe(time.Since(start).Seconds())
		if err != nil {
			log.Warn("read failed", "station", t.id, "err", err)
			cli.Close()
			register(nil)
			cli = nil
			prev = nil
			continue
		}

		cur := &snapshot{
			state:      station.State(regs[station.RegState]),
			cycleMs:    regs[station.RegCycleMs],
			partCount:  regs[station.RegPartCount],
			tempC10:    regs[station.RegTempC10],
			faultCode:  regs[station.RegFaultCode],
			scrapCount: regs[station.RegScrapCount],
			target:     regs[station.RegTargetCycle],
		}
		emitDiff(ctx, sink, t.id, prev, cur)
		prev = cur
	}
}

func emitDiff(ctx context.Context, sink Sink, id string, prev, cur *snapshot) {
	now := time.Now()
	base := events.StationEvent{StationID: id, Timestamp: now, State: cur.state.String(),
		TargetCycleMs: cur.target, PartCount: cur.partCount, ScrapCount: cur.scrapCount,
		TempC: float64(cur.tempC10) / 10, CycleMs: cur.cycleMs, FaultCode: cur.faultCode}

	if prev == nil {
		base.Type = events.TypeStateChange
		sink.Emit(ctx, base)
		return
	}
	if cur.state != prev.state {
		ev := base
		ev.PrevState = prev.state.String()
		switch {
		case cur.state == station.Faulted:
			ev.Type = events.TypeFault
		case prev.state == station.Faulted:
			ev.Type = events.TypeFaultClear
		default:
			ev.Type = events.TypeStateChange
		}
		sink.Emit(ctx, ev)
	}
	if cur.partCount != prev.partCount || cur.scrapCount != prev.scrapCount {
		ev := base
		ev.Type = events.TypeCycle
		sink.Emit(ctx, ev)
	}
	if cur.target != prev.target {
		ev := base
		ev.Type = "recipe_changed"
		sink.Emit(ctx, ev)
	}
}
