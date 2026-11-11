// plc-sim runs N simulated stations, each as a Modbus TCP server on its own port.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hareshgoyal06/andon/pkg/modbus"
	"github.com/hareshgoyal06/andon/pkg/station"
)

func main() {
	stations := flag.Int("stations", 5, "number of stations to simulate")
	basePort := flag.Int("base-port", 5020, "first TCP port; station i listens on base-port+i")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < *stations; i++ {
		cfg := station.DefaultConfig(i + 1)
		cfg.TargetCycleMs = uint16(800 + 100*i) // staggered so a bottleneck emerges naturally
		st := station.New(cfg)

		addr := fmt.Sprintf(":%d", *basePort+i)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("listen", "addr", addr, "err", err)
			os.Exit(1)
		}
		log.Info("station up", "id", cfg.ID, "addr", addr, "target_cycle_ms", cfg.TargetCycleMs)

		wg.Add(2)
		go func() { defer wg.Done(); st.Run(ctx) }()
		go func() {
			defer wg.Done()
			srv := &modbus.Server{Store: st, Log: log}
			if err := srv.Serve(ctx, ln); err != nil {
				log.Error("serve", "addr", addr, "err", err)
			}
		}()
	}

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
}
