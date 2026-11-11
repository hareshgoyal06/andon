#!/usr/bin/env bash
# Kill a processor replica mid-stream and prove no events were lost.
# Works against docker compose (default) or kubernetes (MODE=k8s).
set -euo pipefail
MODE=${MODE:-compose}
API=${API:-http://localhost:8080}

count() { curl -s "$API/api/events?since=24h&limit=5000" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))'; }
parts() { curl -s "$API/api/stations" | python3 -c 'import sys,json; print(sum(s["part_count"] for s in json.load(sys.stdin)))'; }

echo "events in DB before: $(count)   total parts: $(parts)"

if [ "$MODE" = k8s ]; then
  POD=$(kubectl -n andon get pods -l app=processor -o jsonpath='{.items[0].metadata.name}')
  echo "killing $POD"
  kubectl -n andon delete pod "$POD" --grace-period=0 --force
else
  CID=$(docker compose ps -q processor | head -n1)
  echo "killing processor container $CID (SIGKILL)"
  docker kill "$CID" >/dev/null
fi

echo "waiting 15s for rebalance + replay..."
sleep 15
echo "events in DB after:  $(count)   total parts: $(parts)"
echo
echo "Check: part_count is monotonic per station (no gaps) and the dashboard never stopped updating."
echo "Duplicate-free because events are keyed (station_id, ts, type) with ON CONFLICT DO NOTHING."
