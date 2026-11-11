import { useCallback, useEffect, useRef, useState } from "react";
import { api, wsUrl } from "./api";
import type { Alert, OEE, StationEvent, StationState, WsMessage } from "./types";

const MAX_EVENTS = 400;
const MAX_ALERTS = 200;
const MAX_CYCLES = 60;

export type LiveData = {
  stations: Record<string, StationState>;
  oee: Record<string, OEE>;
  alerts: Alert[];
  events: StationEvent[];
  cycles: Record<string, number[]>; // recent cycle times per station
  connected: boolean;
  refresh: () => void;
};

export function useLiveData(): LiveData {
  const [stations, setStations] = useState<Record<string, StationState>>({});
  const [oee, setOee] = useState<Record<string, OEE>>({});
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [events, setEvents] = useState<StationEvent[]>([]);
  const [cycles, setCycles] = useState<Record<string, number[]>>({});
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  const refresh = useCallback(() => {
    api.stations().then((s) => setStations(Object.fromEntries(s.map((x) => [x.station_id, x])))).catch(() => {});
    api.oee().then((o) => setOee(Object.fromEntries(o.map((x) => [x.station_id, x])))).catch(() => {});
    api.alerts().then(setAlerts).catch(() => {});
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 15000); // reconcile in case WS dropped frames
    return () => clearInterval(t);
  }, [refresh]);

  useEffect(() => {
    let closed = false;
    let retry = 1000;

    const connect = () => {
      const ws = new WebSocket(wsUrl());
      wsRef.current = ws;
      ws.onopen = () => {
        setConnected(true);
        retry = 1000;
        refresh();
      };
      ws.onclose = () => {
        setConnected(false);
        if (!closed) {
          setTimeout(connect, retry);
          retry = Math.min(retry * 2, 10000);
        }
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (m) => {
        const msg: WsMessage = JSON.parse(m.data);
        if (msg.channel === "events") {
          const e = msg.data;
          if (e.type === "command_result") return;
          setEvents((prev) => [e, ...prev].slice(0, MAX_EVENTS));
          setStations((prev) => {
            const cur = prev[e.station_id] ?? ({ station_id: e.station_id } as StationState);
            const next: StationState = {
              ...cur,
              state: e.state ?? cur.state,
              online: e.type !== "offline",
              cycle_ms: e.cycle_ms ?? cur.cycle_ms,
              target_cycle_ms: e.target_cycle_ms ?? cur.target_cycle_ms,
              part_count: e.part_count ?? cur.part_count,
              scrap_count: e.scrap_count ?? cur.scrap_count,
              temp_c: e.temp_c ?? cur.temp_c,
              fault_code: e.fault_code ?? 0,
              updated_at: e.ts,
            };
            return { ...prev, [e.station_id]: next };
          });
          if (e.type === "cycle_complete" && e.cycle_ms) {
            setCycles((prev) => ({
              ...prev,
              [e.station_id]: [...(prev[e.station_id] ?? []), e.cycle_ms!].slice(-MAX_CYCLES),
            }));
          }
        } else if (msg.channel === "alerts") {
          setAlerts((prev) => [msg.data, ...prev.filter((a) => a.id !== msg.data.id)].slice(0, MAX_ALERTS));
        } else if (msg.channel === "oee") {
          setOee((prev) => ({ ...prev, [msg.data.station_id]: msg.data }));
        }
      };
    };
    connect();
    return () => {
      closed = true;
      wsRef.current?.close();
    };
  }, [refresh]);

  return { stations, oee, alerts, events, cycles, connected, refresh };
}
