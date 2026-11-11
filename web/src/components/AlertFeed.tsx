import type { Alert } from "../types";
import { ago } from "../format";

export function AlertFeed({ alerts, onSelect }: { alerts: Alert[]; onSelect: (id: string) => void }) {
  if (alerts.length === 0) return <div className="empty">No alerts yet.</div>;
  return (
    <ul className="alert-list">
      {alerts.slice(0, 60).map((a) => (
        <li key={a.id} className={"alert " + a.severity} onClick={() => onSelect(a.station_id)}>
          <span className="alert-sev">{a.severity}</span>
          <span className="alert-station">{a.station_id}</span>
          <span className="alert-msg">{a.message}</span>
          <span className="alert-time">{ago(a.ts)}</span>
        </li>
      ))}
    </ul>
  );
}
