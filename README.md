# Andon

**A real-time factory line control plane.** Go services talk to PLCs over **Modbus TCP**, stream every
state change through **Kafka**, compute **OEE** and detect faults/drift/bottlenecks in a horizontally
scalable stream processor, persist to **TimescaleDB**, and push live updates to a **React** dashboard
over WebSockets. Operators push **process change requests** (recipe changes) and **inject faults** from
the UI, and the command round-trips back to the PLC registers. Deployed with **Docker Compose** or
**Kubernetes (Helm)**, instrumented with **Prometheus + Grafana**, and designed for **zero-loss failover**.

> *Prototypes are easy, manufacturing is hard.*

```
                      Modbus TCP                 Kafka                                 Timescale
 ┌───────────┐  poll 100ms  ┌─────────┐  station.events  ┌───────────┐  idempotent  ┌─────────┐
 │  PLC sims │◀────────────▶│ gateway │────────────────▶│ processor │─────────────▶│   DB    │
 │ (6 stns)  │   write regs │         │◀────────────────│  ×N       │  station.oee │         │
 └───────────┘              └─────────┘ station.commands│  OEE/drift│  station.alerts        │
                                                        └───────────┘────┐         └────┬────┘
                                                                         ▼              │
                                                 ┌──────────┐  WebSocket ┌─────┐  REST   │
                                                 │ React UI │◀──────────│ api │◀────────┘
                                                 └──────────┘  commands ▶└─────┘
```

## What it demonstrates

| Capability | Where |
|---|---|
| **Modbus TCP implemented from scratch** (MBAP framing, FC 0x03/0x06/0x10, exceptions) | `pkg/modbus` |
| PLC simulation: state machine, cycle jitter, scrap, spontaneous + injected faults | `pkg/station`, `cmd/plc-sim` |
| Protocol gateway: per-station pollers, reconnect w/ backoff, register diffing → events | `cmd/gateway` |
| **Stream processing**: rolling-window OEE, 2σ cycle-drift detection, bottleneck detection, alerting | `pkg/oee`, `cmd/processor` |
| **Fault tolerance**: consumer groups, at-least-once + idempotent writes = effectively-once; state rebuild from DB on restart | `cmd/processor`, `pkg/db` |
| **Process change requests**: UI → REST → Kafka → gateway → Modbus write → PLC, with audit log + result tracking | `cmd/api`, `cmd/gateway`, `commands` table |
| Live dashboard: line view, OEE bars, sparklines, alert feed, event stream, recipe editor, chaos button | `web/` |
| Observability: Prometheus metrics on every service, provisioned Grafana dashboard | `deploy/prometheus`, `deploy/grafana` |
| Deployment: multi-target distroless Dockerfile, Compose, Helm chart w/ probes, PDB, HPA-ready | `Dockerfile`, `docker-compose.yml`, `deploy/helm` |

## Run it (Docker Compose)

Prereqs: Docker Desktop / OrbStack / Colima with `docker compose` v2.

```bash
git clone https://github.com/hareshgoyal06/andon && cd andon
docker compose up -d --build        # ~2-3 min first time
```

| URL | What |
|---|---|
| http://localhost:8080 | **Dashboard** |
| http://localhost:3000 | Grafana (admin / admin) → Andon → Line Overview |
| http://localhost:9090 | Prometheus |
| http://localhost:8080/api/stations | REST API |
| http://localhost:8080/metrics | API metrics (gateway :9101, processors :9102) |

Within ~10 s the six stations come online and start cycling. Within ~30 s OEE snapshots appear.
Faults fire spontaneously (~1%/cycle) and auto-clear after 5 s.

### Things to try

```bash
# Watch events flow in real time
docker compose logs -f gateway processor

# Push a process change: station-3 should run a 600 ms cycle
curl -X POST localhost:8080/api/stations/station-3/recipe \
     -H 'Content-Type: application/json' -d '{"target_cycle_ms": 600}'

# Inject a fault (chaos) on station-5
curl -X POST localhost:8080/api/stations/station-5/chaos

# Audit log: every command, who issued it, whether the PLC accepted it
curl -s localhost:8080/api/commands | jq .

# Failover: SIGKILL a processor replica mid-stream, verify nothing is lost
make failover

# Scale processors live; Kafka rebalances partitions
docker compose up -d --scale processor=4

# Kill the PLCs: gateway reports offline, dashboard goes grey, alerts fire; bring them back
docker compose stop plc-sim && sleep 10 && docker compose start plc-sim

# Watch raw Modbus on the wire
docker run --rm --net container:$(docker compose ps -q plc-sim) nicolaka/netshoot \
  tcpdump -i any -nn -X 'tcp port 5020' | head -60
```

## Run it (Kubernetes)

```bash
brew install kind helm
make kind-up                 # builds images, creates cluster, helm installs, waits for ready
open http://localhost:30080  # dashboard
open http://localhost:30300  # grafana

# failover demo on k8s
MODE=k8s make failover
```

The chart deploys 3 processor replicas with a PodDisruptionBudget, readiness/liveness probes on every
service, single-node Kafka (KRaft) and TimescaleDB StatefulSets, and Prometheus with pod discovery.

## Local dev (no Docker)

```bash
make run-plc                 # terminal 1: 6 PLCs on :5020-5025
make run-gateway-stdout      # terminal 2: events as JSON lines, no Kafka required
go test -race ./...
cd web && npm i && npm run dev   # dashboard on :5173 proxying to an api on :8080
```

## Design notes

**Why Modbus from scratch?** It's ~300 lines and it means I understand every byte on the wire — which
is exactly what you need when a station stops responding at 3 a.m. and all you have is a pcap.

**Ordering & exactly-once.** Events are keyed by `station_id`, so each station's stream is totally
ordered within one Kafka partition. Consumers commit offsets only after a successful DB write; the DB
primary key `(station_id, ts, type)` with `ON CONFLICT DO NOTHING` makes redelivery harmless. Net effect:
at-least-once delivery, effectively-once results.

**State rebuild.** The processor's OEE window is in-memory for speed, but it's derived state. On start
each replica replays the last 5 minutes of events from TimescaleDB, so a crash or rebalance never
produces a dip in OEE.

**Command path is asynchronous and auditable.** `POST /recipe` returns `202 Accepted` with a command id.
The gateway executes it against the PLC and emits a `command_result` event; the processor marks the
command `applied` or `failed`. Operators can see exactly what was asked, by whom, and whether the PLC
took it. Writes to read-only registers are rejected by the PLC with a Modbus exception — and that error
propagates all the way back.

**Backpressure.** The WebSocket hub drops frames for slow clients instead of blocking Kafka consumption.
The UI reconciles from REST every 15 s so a dropped frame is cosmetic, never a correctness problem.

**OEE definitions.** Availability = 1 − downtime/window (faulted or offline). Performance = target cycle
÷ mean actual cycle, capped at 1. Quality = good ÷ (good + scrap). Window is 5 min rolling.
Bottleneck = slowest station by mean cycle time if >15% slower than the line median.

## Modbus register map (per station)

| Addr | Register | R/W | Meaning |
|---|---|---|---|
| 0 | STATE | R | 0=idle 1=running 2=faulted 3=blocked |
| 1 | CYCLE_MS | R | last cycle time |
| 2 | PART_COUNT | R | good parts (wraps at 65535) |
| 3 | TEMP_C10 | R | temperature × 10 |
| 4 | FAULT_CODE | R | 0 = none; 100–104 = fault id |
| 5 | SCRAP_COUNT | R | scrapped parts |
| 10 | TARGET_CYCLE_MS | RW | recipe: target cycle time (min 50) |
| 11 | FAULT_INJECT | RW | write 1 to force a fault |

## REST API

| Method | Path | Description |
|---|---|---|
| GET | `/api/stations` | current state of every station |
| GET | `/api/stations/{id}/events?since=15m&limit=500` | event history |
| GET | `/api/stations/{id}/oee/history?since=30m` | OEE snapshots |
| GET | `/api/oee` | latest OEE per station |
| GET | `/api/alerts?limit=100` | recent alerts |
| GET | `/api/commands?limit=100` | command audit log |
| GET | `/api/events?since=10m` | all recent events |
| POST | `/api/stations/{id}/recipe` `{"target_cycle_ms": N}` | process change request |
| POST | `/api/stations/{id}/chaos` | inject a fault |
| WS | `/ws` | `{channel: events\|alerts\|oee, data}` stream |

## Layout

```
cmd/plc-sim/       simulated PLCs (Modbus TCP servers)
cmd/gateway/       Modbus poller → Kafka; Kafka commands → Modbus writes
cmd/processor/     Kafka → OEE/alerts → Timescale + Kafka
cmd/api/           REST + WebSocket + static dashboard
pkg/modbus/        Modbus TCP protocol (client + server)
pkg/station/       PLC state machine + register bank
pkg/oee/           OEE tracker, drift + bottleneck detection
pkg/kafkax/        topic bootstrap, producer, consumer-group helpers
pkg/db/            Timescale schema + queries
pkg/events/        shared message types
web/               React + Vite + TypeScript dashboard
deploy/helm/       Helm chart (incl. Kafka, Timescale, Prometheus, Grafana)
deploy/kind/       kind cluster config
deploy/grafana/    provisioned dashboard
scripts/           kind-up, failover-demo, load-test
```

## Tests

```bash
go test -race ./...
```

- `pkg/modbus`: frame round-trip, client↔server, exception codes, 20 concurrent clients
- `pkg/station`: produces parts, fault injection via Modbus write, recipe push via Modbus write, RO register rejection
- `pkg/oee`: perfect station, availability from faults, ongoing fault, quality & performance, drift alert, recipe resets baseline, window pruning, bottleneck detection

## License

MIT
