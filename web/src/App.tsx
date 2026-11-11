import { useMemo, useState } from "react";
import { useLiveData } from "./useLiveData";
import { LineView } from "./components/LineView";
import { StationDetail } from "./components/StationDetail";
import { AlertFeed } from "./components/AlertFeed";
import { LineSummary } from "./components/LineSummary";
import { EventStream } from "./components/EventStream";

export default function App() {
  const data = useLiveData();
  const [selected, setSelected] = useState<string | null>(null);

  const stationIds = useMemo(
    () => Object.keys(data.stations).sort((a, b) => a.localeCompare(b, undefined, { numeric: true })),
    [data.stations]
  );

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" />
          <h1>ANDON</h1>
          <span className="sub">Line 1 · Fremont</span>
        </div>
        <div className="status">
          <span className={"dot " + (data.connected ? "ok" : "bad")} />
          {data.connected ? "live" : "reconnecting…"}
        </div>
      </header>

      <LineSummary stations={data.stations} oee={data.oee} alerts={data.alerts} />

      <main className="grid">
        <section className="panel line">
          <h2>Line view</h2>
          <LineView
            ids={stationIds}
            stations={data.stations}
            oee={data.oee}
            cycles={data.cycles}
            selected={selected}
            onSelect={setSelected}
          />
        </section>

        <section className="panel detail">
          {selected && data.stations[selected] ? (
            <StationDetail
              station={data.stations[selected]}
              oee={data.oee[selected]}
              cycles={data.cycles[selected] ?? []}
              onClose={() => setSelected(null)}
            />
          ) : (
            <div className="empty">
              <h2>Station detail</h2>
              <p>Select a station to view OEE history, push a recipe change, or inject a fault.</p>
            </div>
          )}
        </section>

        <section className="panel alerts">
          <h2>Alerts</h2>
          <AlertFeed alerts={data.alerts} onSelect={setSelected} />
        </section>

        <section className="panel events">
          <h2>Event stream</h2>
          <EventStream events={data.events} />
        </section>
      </main>
    </div>
  );
}
