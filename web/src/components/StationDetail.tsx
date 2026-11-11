import { useEffect, useState } from "react";
import type { OEE, StationState } from "../types";
import { api } from "../api";
import { Sparkline } from "./Sparkline";
import { ms, pct } from "../format";

type Props = { station: StationState; oee?: OEE; cycles: number[]; onClose: () => void };

export function StationDetail({ station, oee, cycles, onClose }: Props) {
  const [target, setTarget] = useState(station.target_cycle_ms || 1000);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [history, setHistory] = useState<OEE[]>([]);

  useEffect(() => setTarget(station.target_cycle_ms || 1000), [station.station_id, station.target_cycle_ms]);

  useEffect(() => {
    let alive = true;
    const load = () => api.oeeHistory(station.station_id, "30m").then((h) => alive && setHistory(h)).catch(() => {});
    load();
    const t = setInterval(load, 10000);
    return () => { alive = false; clearInterval(t); };
  }, [station.station_id]);

  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(label);
    setMsg(null);
    try {
      await fn();
      setMsg(`${label} queued → gateway → Modbus`);
    } catch (e) {
      setMsg(`Error: ${(e as Error).message}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="detail-inner">
      <div className="detail-head">
        <h2>{station.station_id}</h2>
        <button className="ghost" onClick={onClose}>✕</button>
      </div>

      <div className="oee-bars">
        <Bar label="Availability" v={oee?.availability} />
        <Bar label="Performance" v={oee?.performance} />
        <Bar label="Quality" v={oee?.quality} />
        <Bar label="OEE" v={oee?.oee} strong />
      </div>

      <dl className="facts">
        <dt>Window</dt><dd>{oee ? `${oee.window_sec / 60} min` : "—"}</dd>
        <dt>Avg cycle</dt><dd>{ms(oee?.avg_cycle_ms)}</dd>
        <dt>Parts / scrap</dt><dd>{oee ? `${oee.parts} / ${oee.scrap}` : "—"}</dd>
        <dt>Faults in window</dt><dd>{oee?.faults ?? "—"}</dd>
        <dt>Temperature</dt><dd>{station.temp_c ? `${station.temp_c.toFixed(1)} °C` : "—"}</dd>
        <dt>Lifetime parts</dt><dd>{station.part_count}</dd>
      </dl>

      <h3>Cycle time (last {cycles.length})</h3>
      <Sparkline values={cycles} target={station.target_cycle_ms} width={320} height={60} />

      <h3>OEE trend (30 min)</h3>
      <Sparkline values={history.map((h) => h.oee)} width={320} height={60} />

      <h3>Process change request</h3>
      <div className="recipe">
        <label>
          Target cycle (ms)
          <input type="number" min={50} max={60000} step={50} value={target} onChange={(e) => setTarget(Number(e.target.value))} />
        </label>
        <button disabled={busy !== null} onClick={() => run("Recipe change", () => api.setRecipe(station.station_id, target))}>
          Push recipe
        </button>
      </div>

      <h3>Chaos</h3>
      <button className="danger" disabled={busy !== null} onClick={() => run("Fault injection", () => api.injectFault(station.station_id))}>
        Inject fault
      </button>
      {msg && <p className="msg">{msg}</p>}
    </div>
  );
}

function Bar({ label, v, strong }: { label: string; v?: number; strong?: boolean }) {
  const w = v == null ? 0 : Math.round(v * 100);
  const tone = v == null ? "" : v > 0.85 ? "ok" : v > 0.6 ? "warn" : "bad";
  return (
    <div className={"bar " + (strong ? "strong" : "")}>
      <div className="bar-label"><span>{label}</span><b>{pct(v)}</b></div>
      <div className="bar-track"><div className={"bar-fill " + tone} style={{ width: `${w}%` }} /></div>
    </div>
  );
}
