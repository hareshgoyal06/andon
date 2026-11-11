# Andon

A real-time factory line control plane: Go + Kafka + React, talking to simulated PLCs over **Modbus TCP**,
computing OEE and detecting faults from the event stream, and hot-pushing process changes back to the line.
Deployed on Kubernetes with zero-loss failover.

> *Prototypes are easy, manufacturing is hard.*

```
[PLC sims (Go)] --Modbus TCP--> [Gateway (Go)] --> Kafka --> [Processor (Go)] --> TimescaleDB
                                                                   |
                                                                   v
                                                          [API + WebSocket (Go)] --> [React dashboard]
```

## Status

- [x] Week 1 — Modbus TCP implemented from scratch (`pkg/modbus`), PLC station simulator with state machine + fault injection, polling gateway that diffs registers and emits change events
- [ ] Week 2 — Kafka producer in gateway; stream processor computing OEE + bottleneck detection
- [ ] Week 3 — REST/WebSocket API + React dashboard
- [ ] Week 4 — Recipe push round-trip (UI → Kafka → Modbus write) + chaos toggle
- [ ] Week 5 — Helm chart, kind cluster, failover demo, Grafana
- [ ] Week 6 — Architecture write-up, GIFs, design tradeoffs

## Quick start

```bash
# terminal 1: 5 simulated stations on ports 5020-5024
make run-plc

# terminal 2: poll them and print change events
make run-gateway
```

## Modbus register map (per station)

| Address | Register | Access | Meaning |
|---|---|---|---|
| 0 | STATE | R | 0=idle 1=running 2=faulted 3=blocked |
| 1 | CYCLE_MS | R | last cycle time in ms |
| 2 | PART_COUNT | R | parts completed (wraps at 65535) |
| 3 | TEMP_C10 | R | temperature × 10 |
| 4 | FAULT_CODE | R | 0=none, else fault id |
| 10 | TARGET_CYCLE_MS | RW | recipe: target cycle time |
| 11 | FAULT_INJECT | RW | write 1 to force a fault (chaos) |

Function codes supported: `0x03` Read Holding Registers, `0x06` Write Single Register, `0x10` Write Multiple Registers.

## Layout

```
cmd/plc-sim/     station simulator (Modbus TCP server)
cmd/gateway/     poller → change events (→ Kafka in week 2)
pkg/modbus/      Modbus TCP framing, client + server
pkg/station/     station state machine
pkg/events/      event types shared across services
```
