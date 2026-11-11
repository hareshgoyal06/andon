import type { OEE, StationState } from "../types";
import { Sparkline } from "./Sparkline";
import { ms, pct } from "../format";

type Props = {
  ids: string[];
  stations: Record<string, StationState>;
  oee: Record<string, OEE>;
  cycles: Record<string, number[]>;
  selected: string | null;
  onSelect: (id: string) => void;
};

export function LineView({ ids, stations, oee, cycles, selected, onSelect }: Props) {
  if (ids.length === 0) return <div className="empty">Waiting for stations…</div>;
  return (
    <div className="line-view">
      {ids.map((id, i) => {
        const s = stations[id];
        const o = oee[id];
        const tone = !s.online ? "offline" : s.state === "faulted" ? "faulted" : s.state === "running" ? "running" : "idle";
        return (
          <div key={id} className="station-wrap">
            <button className={`station ${tone} ${selected === id ? "selected" : ""}`} onClick={() => onSelect(id)}>
              <div className="station-head">
                <span className="station-id">{id}</span>
                {o?.is_bottleneck && <span className="badge bottleneck">BOTTLENECK</span>}
              </div>
              <div className="station-state">{s.online ? s.state : "offline"}</div>
              {s.state === "faulted" && s.fault_code ? <div className="fault-code">F{s.fault_code}</div> : null}
              <Sparkline values={cycles[id] ?? []} target={s.target_cycle_ms} />
              <div className="station-stats">
                <div><span>OEE</span><b>{pct(o?.oee)}</b></div>
                <div><span>Cycle</span><b>{ms(s.cycle_ms)}</b></div>
                <div><span>Target</span><b>{ms(s.target_cycle_ms)}</b></div>
                <div><span>Parts</span><b>{s.part_count ?? 0}</b></div>
              </div>
            </button>
            {i < ids.length - 1 && <div className="conveyor" />}
          </div>
        );
      })}
    </div>
  );
}
