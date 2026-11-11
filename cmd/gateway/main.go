// gateway polls stations over Modbus TCP, diffs register snapshots, and emits
// StationEvents. Today it prints JSON lines; week 2 swaps the sink for Kafka.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hareshgoyal06/andon/pkg/events"
	"github.com/hareshgoyal06/andon/pkg/modbus"
	"github.com/hareshgoyal06/andon/pkg/station"
)

// Sink receives events. Implemented by stdout today, Kafka next.
type Sink interface {
	Emit(ev events.StationEvent)
}

type stdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (s *stdoutSink) Emit(ev events.StationEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(ev)
}

func main() {
	stationsFlag := flag.String("stations", "localhost:5020", "comma-separated host:port list")
	interval := flag.Duration("interval", 100*time.Millisecond, "poll interval")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sink := &stdoutSink{enc: json.NewEncoder(os.Stdout)}

	var wg sync.WaitGroup
	for _, addr := range strings.Split(*stationsFlag, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			pollStation(ctx, log, sink, addr, *interval)
		}(addr)
	}
	wg.Wait()
}

// snapshot is the subset of registers we track for diffing.
type snapshot struct {
	state     station.State
	cycleMs   uint16
	partCount uint16
	tempC10   uint16
	faultCode uint16
}

func pollStation(ctx context.Context, log *slog.Logger, sink Sink, addr string, interval time.Duration) {
	id := addr
	var (
		cli    *modbus.Client
		prev   *snapshot
		online bool
	)
	backoff := time.Second

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			if cli != nil {
				cli.Close()
			}
			return
		case <-t.C:
		}

		if cli == nil {
			cli = &modbus.Client{Addr: addr, Unit: 1}
			if err := cli.Dial(ctx); err != nil {
				cli = nil
				if online {
					online = false
					sink.Emit(events.StationEvent{Type: events.TypeOffline, StationID: id, Timestamp: time.Now(), Error: err.Error()})
				}
				log.Warn("dial failed", "station", id, "err", err, "retry_in", backoff)
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
			if !online {
				online = true
				sink.Emit(events.StationEvent{Type: events.TypeOnline, StationID: id, Timestamp: time.Now()})
			}
		}

		regs, err := cli.ReadHoldingRegisters(0, 5)
		if err != nil {
			log.Warn("read failed", "station", id, "err", err)
			cli.Close()
			cli = nil
			prev = nil
			continue
		}

		cur := &snapshot{
			state:     station.State(regs[station.RegState]),
			cycleMs:   regs[station.RegCycleMs],
			partCount: regs[station.RegPartCount],
			tempC10:   regs[station.RegTempC10],
			faultCode: regs[station.RegFaultCode],
		}
		emitDiff(sink, id, prev, cur)
		prev = cur
	}
}

func emitDiff(sink Sink, id string, prev, cur *snapshot) {
	now := time.Now()
	if prev == nil {
		sink.Emit(events.StationEvent{Type: events.TypeStateChange, StationID: id, Timestamp: now, State: cur.state.String(), PartCount: cur.partCount, TempC: float64(cur.tempC10) / 10})
		return
	}
	if cur.state != prev.state {
		switch {
		case cur.state == station.Faulted:
			sink.Emit(events.StationEvent{Type: events.TypeFault, StationID: id, Timestamp: now, State: cur.state.String(), PrevState: prev.state.String(), FaultCode: cur.faultCode})
		case prev.state == station.Faulted:
			sink.Emit(events.StationEvent{Type: events.TypeFaultClear, StationID: id, Timestamp: now, State: cur.state.String(), PrevState: prev.state.String()})
		default:
			sink.Emit(events.StationEvent{Type: events.TypeStateChange, StationID: id, Timestamp: now, State: cur.state.String(), PrevState: prev.state.String()})
		}
	}
	if cur.partCount != prev.partCount {
		sink.Emit(events.StationEvent{Type: events.TypeCycle, StationID: id, Timestamp: now, State: cur.state.String(), CycleMs: cur.cycleMs, PartCount: cur.partCount, TempC: float64(cur.tempC10) / 10})
	}
}
