import type { Alert, OEE, StationState } from "../types";
import { pct } from "../format";

export function LineSummary({ stations, oee, alerts }: { stations: Record<string, StationState>; oee: Record<string, OEE>; alerts: Alert[] }) {
  const list = Object.values(stations);
  const running = list.filter((s) => s.state === "running" && s.online).length;
  const faulted = list.filter((s) => s.state === "faulted").length;
  const offline = list.filter((s) => !s.online).length;
  const oees = Object.values(oee);
  const lineOee = oees.length ? oees.reduce((a, o) => a + o.oee, 0) / oees.length : 0;
  const parts = list.reduce((a, s) => a + (s.part_count ?? 0), 0);
  const bottleneck = oees.find((o) => o.is_bottleneck)?.station_id;
  const critical = alerts.filter((a) => a.severity === "critical" && Date.now() - Date.parse(a.ts) < 5 * 60_000).length;

  return (
    <div className="summary">
      <Kpi label="Line OEE" value={pct(lineOee)} tone={lineOee > 0.85 ? "ok" : lineOee > 0.6 ? "warn" : "bad"} />
      <Kpi label="Stations" value={`${running}/${list.length}`} hint="running" tone={faulted || offline ? "warn" : "ok"} />
      <Kpi label="Faulted" value={String(faulted)} tone={faulted ? "bad" : "ok"} />
      <Kpi label="Offline" value={String(offline)} tone={offline ? "bad" : "ok"} />
      <Kpi label="Parts (session)" value={parts.toLocaleString()} />
      <Kpi label="Bottleneck" value={bottleneck ?? "—"} tone={bottleneck ? "warn" : undefined} />
      <Kpi label="Critical (5m)" value={String(critical)} tone={critical ? "bad" : "ok"} />
    </div>
  );
}

function Kpi({ label, value, hint, tone }: { label: string; value: string; hint?: string; tone?: "ok" | "warn" | "bad" }) {
  return (
    <div className={"kpi " + (tone ?? "")}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  );
}
