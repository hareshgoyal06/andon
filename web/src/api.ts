import type { Alert, CommandRecord, OEE, StationEvent, StationState } from "./types";

async function get<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json();
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const r = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) {
    const e = await r.json().catch(() => ({ error: r.statusText }));
    throw new Error(e.error ?? r.statusText);
  }
  return r.json();
}

export const api = {
  stations: () => get<StationState[]>("/api/stations"),
  oee: () => get<OEE[]>("/api/oee"),
  alerts: (limit = 100) => get<Alert[]>(`/api/alerts?limit=${limit}`),
  commands: (limit = 50) => get<CommandRecord[]>(`/api/commands?limit=${limit}`),
  events: (station: string, since = "15m", limit = 500) =>
    get<StationEvent[]>(`/api/stations/${station}/events?since=${since}&limit=${limit}`),
  oeeHistory: (station: string, since = "30m") => get<OEE[]>(`/api/stations/${station}/oee/history?since=${since}`),
  setRecipe: (station: string, target_cycle_ms: number) =>
    post(`/api/stations/${station}/recipe`, { target_cycle_ms }),
  injectFault: (station: string) => post(`/api/stations/${station}/chaos`),
};

export function wsUrl(): string {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}/ws`;
}
