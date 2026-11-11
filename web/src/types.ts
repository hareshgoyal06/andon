export type StationState = {
  station_id: string;
  state: "idle" | "running" | "faulted" | "blocked" | string;
  online: boolean;
  cycle_ms: number;
  target_cycle_ms: number;
  part_count: number;
  scrap_count: number;
  temp_c: number;
  fault_code: number;
  updated_at: string;
};

export type StationEvent = {
  type: string;
  station_id: string;
  ts: string;
  state?: string;
  prev_state?: string;
  cycle_ms?: number;
  target_cycle_ms?: number;
  part_count?: number;
  scrap_count?: number;
  temp_c?: number;
  fault_code?: number;
  error?: string;
};

export type Alert = {
  id: string;
  station_id: string;
  severity: "info" | "warning" | "critical";
  kind: string;
  message: string;
  ts: string;
};

export type OEE = {
  station_id: string;
  ts: string;
  window_sec: number;
  availability: number;
  performance: number;
  quality: number;
  oee: number;
  avg_cycle_ms: number;
  parts: number;
  scrap: number;
  faults: number;
  is_bottleneck: boolean;
};

export type CommandRecord = {
  id: string;
  type: string;
  station_id: string;
  value?: number;
  actor?: string;
  ts: string;
  status: string;
  error?: string;
  applied_at?: string;
};

export type WsMessage =
  | { channel: "events"; data: StationEvent }
  | { channel: "alerts"; data: Alert }
  | { channel: "oee"; data: OEE };
