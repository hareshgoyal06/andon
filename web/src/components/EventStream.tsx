import type { StationEvent } from "../types";
import { time } from "../format";

function describe(e: StationEvent): string {
  switch (e.type) {
    case "cycle_complete": return `cycle ${e.cycle_ms} ms · parts ${e.part_count}${e.scrap_count ? ` · scrap ${e.scrap_count}` : ""}`;
    case "fault": return `FAULT F${e.fault_code} (${e.prev_state} → faulted)`;
    case "fault_cleared": return `fault cleared → ${e.state}`;
    case "state_change": return `${e.prev_state ?? "?"} → ${e.state}`;
    case "offline": return `OFFLINE: ${e.error}`;
    case "online": return "ONLINE";
    case "recipe_changed": return `recipe → target ${e.target_cycle_ms} ms`;
    default: return e.type;
  }
}

export function EventStream({ events }: { events: StationEvent[] }) {
  if (events.length === 0) return <div className="empty">Waiting for events…</div>;
  return (
    <ul className="event-list">
      {events.slice(0, 120).map((e, i) => (
        <li key={`${e.station_id}-${e.ts}-${i}`} className={"ev " + e.type}>
          <span className="ev-time">{time(e.ts)}</span>
          <span className="ev-station">{e.station_id}</span>
          <span className="ev-type">{e.type}</span>
          <span className="ev-desc">{describe(e)}</span>
        </li>
      ))}
    </ul>
  );
}
