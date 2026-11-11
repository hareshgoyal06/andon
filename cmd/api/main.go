// api serves the REST + WebSocket interface for the dashboard and publishes
// operator commands (recipe changes, fault injection) to Kafka for the gateway.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/hareshgoyal06/andon/pkg/db"
	"github.com/hareshgoyal06/andon/pkg/events"
	"github.com/hareshgoyal06/andon/pkg/kafkax"
)

var (
	wsClients = promauto.NewGauge(prometheus.GaugeOpts{Name: "andon_api_ws_clients", Help: "Connected websocket clients"})
	httpReqs  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "andon_api_requests_total", Help: "HTTP requests"}, []string{"route", "code"})
	cmdsTotal = promauto.NewCounterVec(prometheus.CounterOpts{Name: "andon_api_commands_total", Help: "Commands issued"}, []string{"type"})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// wsMessage is the envelope pushed to dashboard clients.
type wsMessage struct {
	Channel string          `json:"channel"` // events | alerts | oee
	Data    json.RawMessage `json:"data"`
}

// hub fans Kafka messages out to websocket clients.
type hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]chan []byte
}

func newHub() *hub { return &hub{clients: map[*websocket.Conn]chan []byte{}} }

func (h *hub) add(c *websocket.Conn) chan []byte {
	ch := make(chan []byte, 256)
	h.mu.Lock()
	h.clients[c] = ch
	h.mu.Unlock()
	wsClients.Set(float64(len(h.clients)))
	return ch
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	if ch, ok := h.clients[c]; ok {
		close(ch)
		delete(h.clients, c)
	}
	n := len(h.clients)
	h.mu.Unlock()
	wsClients.Set(float64(n))
}

func (h *hub) broadcast(channel string, raw []byte) {
	b, _ := json.Marshal(wsMessage{Channel: channel, Data: raw})
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- b:
		default: // slow client: drop rather than block the pipeline
		}
	}
}

type server struct {
	log      *slog.Logger
	db       *db.DB
	hub      *hub
	commands *kafkax.Producer
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stations", s.listStations)
	mux.HandleFunc("GET /api/stations/{id}/events", s.stationEvents)
	mux.HandleFunc("GET /api/stations/{id}/oee/history", s.oeeHistory)
	mux.HandleFunc("POST /api/stations/{id}/recipe", s.setRecipe)
	mux.HandleFunc("POST /api/stations/{id}/chaos", s.injectFault)
	mux.HandleFunc("GET /api/alerts", s.listAlerts)
	mux.HandleFunc("GET /api/oee", s.listOEE)
	mux.HandleFunc("GET /api/commands", s.listCommands)
	mux.HandleFunc("GET /api/events", s.allEvents)
	mux.HandleFunc("GET /ws", s.ws)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	return mux
}

func (s *server) listStations(w http.ResponseWriter, r *http.Request) {
	st, err := s.db.ListStationState(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if st == nil {
		st = []db.StationState{}
	}
	writeJSON(w, 200, st)
}

func sinceParam(r *http.Request, def time.Duration) time.Time {
	if v := r.URL.Query().Get("since"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return time.Now().Add(-d)
		}
	}
	return time.Now().Add(-def)
}

func limitParam(r *http.Request, def, max int) int {
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= max {
			return n
		}
	}
	return def
}

func (s *server) stationEvents(w http.ResponseWriter, r *http.Request) {
	evs, err := s.db.RecentEvents(r.Context(), r.PathValue("id"), sinceParam(r, time.Hour), limitParam(r, 200, 5000))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if evs == nil {
		evs = []events.StationEvent{}
	}
	writeJSON(w, 200, evs)
}

func (s *server) allEvents(w http.ResponseWriter, r *http.Request) {
	evs, err := s.db.RecentEvents(r.Context(), "", sinceParam(r, 10*time.Minute), limitParam(r, 200, 5000))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if evs == nil {
		evs = []events.StationEvent{}
	}
	writeJSON(w, 200, evs)
}

func (s *server) oeeHistory(w http.ResponseWriter, r *http.Request) {
	h, err := s.db.OEEHistory(r.Context(), r.PathValue("id"), sinceParam(r, time.Hour))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if h == nil {
		h = []events.OEE{}
	}
	writeJSON(w, 200, h)
}

func (s *server) listAlerts(w http.ResponseWriter, r *http.Request) {
	a, err := s.db.RecentAlerts(r.Context(), limitParam(r, 100, 1000))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if a == nil {
		a = []events.Alert{}
	}
	writeJSON(w, 200, a)
}

func (s *server) listOEE(w http.ResponseWriter, r *http.Request) {
	o, err := s.db.LatestOEE(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if o == nil {
		o = []events.OEE{}
	}
	writeJSON(w, 200, o)
}

func (s *server) listCommands(w http.ResponseWriter, r *http.Request) {
	c, err := s.db.RecentCommands(r.Context(), limitParam(r, 100, 1000))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if c == nil {
		c = []db.CommandRecord{}
	}
	writeJSON(w, 200, c)
}

func (s *server) publishCommand(ctx context.Context, cmd events.Command) error {
	cmd.ID = newID()
	cmd.Timestamp = time.Now()
	cmdsTotal.WithLabelValues(string(cmd.Type)).Inc()
	if err := s.db.InsertCommand(ctx, cmd); err != nil {
		return err
	}
	return s.commands.Publish(ctx, cmd.StationID, cmd)
}

func actor(r *http.Request) string {
	if a := r.Header.Get("X-Actor"); a != "" {
		return a
	}
	return "dashboard"
}

func (s *server) setRecipe(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TargetCycleMs int `json:"target_cycle_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TargetCycleMs < 50 || body.TargetCycleMs > 60000 {
		writeErr(w, 400, "target_cycle_ms must be between 50 and 60000")
		return
	}
	cmd := events.Command{Type: events.CmdSetTargetCycle, StationID: r.PathValue("id"), Value: uint16(body.TargetCycleMs), Actor: actor(r)}
	if err := s.publishCommand(r.Context(), cmd); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"status": "queued", "command": cmd})
}

func (s *server) injectFault(w http.ResponseWriter, r *http.Request) {
	cmd := events.Command{Type: events.CmdInjectFault, StationID: r.PathValue("id"), Value: 1, Actor: actor(r)}
	if err := s.publishCommand(r.Context(), cmd); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"status": "queued", "command": cmd})
}

func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ch := s.hub.add(c)
	defer s.hub.remove(c)
	ctx := r.Context()

	// Drain reads so pings/close frames are processed.
	go func() {
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-ch:
			if !ok {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// fanout tails a topic from the latest offset and broadcasts to the hub.
func (s *server) fanout(ctx context.Context, brokers []string, topic, channel string) {
	// Unique group per API instance so every replica sees every message.
	r := kafkax.NewReader(kafkax.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: "andon-api-" + newID(), FromLatest: true})
	defer r.Close()
	err := kafkax.Consume(ctx, r, func(_ context.Context, m kafka.Message) error {
		s.hub.broadcast(channel, m.Value)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		s.log.Error("fanout", "topic", topic, "err", err)
	}
}

// spaHandler serves the built dashboard with index.html fallback for client routing.
func spaHandler(dir string) http.Handler {
	fs := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := filepath.Join(dir, filepath.Clean(r.URL.Path))
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	})
}

func main() {
	addr := flag.String("addr", env("ADDR", ":8080"), "listen address")
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka brokers")
	dsn := flag.String("dsn", env("DATABASE_URL", "postgres://andon:andon@localhost:5432/andon"), "Postgres DSN")
	staticDir := flag.String("static", env("STATIC_DIR", ""), "directory with built dashboard; empty = API only")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bl := kafkax.Brokers(*brokers)
	if err := kafkax.EnsureTopics(ctx, log, bl, 6, kafkax.TopicEvents, kafkax.TopicCommands, kafkax.TopicAlerts, kafkax.TopicOEE); err != nil {
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

	s := &server{log: log, db: database, hub: newHub(), commands: kafkax.NewProducer(bl, kafkax.TopicCommands)}
	defer s.commands.Close()

	go s.fanout(ctx, bl, kafkax.TopicEvents, "events")
	go s.fanout(ctx, bl, kafkax.TopicAlerts, "alerts")
	go s.fanout(ctx, bl, kafkax.TopicOEE, "oee")

	root := http.NewServeMux()
	root.Handle("/api/", s.routes())
	root.Handle("/ws", s.routes())
	root.Handle("/metrics", s.routes())
	root.Handle("/healthz", s.routes())
	if *staticDir != "" {
		root.Handle("/", spaHandler(*staticDir))
		log.Info("serving dashboard", "dir", *staticDir)
	}

	hs := &http.Server{Addr: *addr, Handler: logging(log, cors(root)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("api listening", "addr", *addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
	log.Info("shutdown complete")
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Actor")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" { // hijacked; cannot wrap
			next.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w, code: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		route := r.URL.Path
		if strings.HasPrefix(route, "/api/") {
			httpReqs.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
			log.Info("http", "method", r.Method, "path", route, "code", sw.code, "ms", time.Since(start).Milliseconds())
		}
	})
}
