// plc-sim runs N simulated stations, each as a Modbus TCP server on its own port.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/hareshgoyal06/andon/pkg/modbus"
	"github.com/hareshgoyal06/andon/pkg/station"
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	stations := flag.Int("stations", envInt("STATIONS", 6), "number of stations to simulate")
	basePort := flag.Int("base-port", envInt("BASE_PORT", 5020), "first TCP port; station i listens on base-port+i")
	healthAddr := flag.String("health-addr", ":8080", "health endpoint address")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < *stations; i++ {
		cfg := station.DefaultConfig(i + 1)
		cfg.TargetCycleMs = uint16(800 + 80*i) // staggered so a bottleneck emerges naturally
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	hs := &http.Server{Addr: *healthAddr, Handler: mux}
	go func() { _ = hs.ListenAndServe() }()

	<-ctx.Done()
	log.Info("shutting down")
	_ = hs.Close()
	wg.Wait()
}
