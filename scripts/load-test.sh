#!/usr/bin/env bash
# Hammer the command path: push a recipe to every station, then inject faults.
set -euo pipefail
API=${API:-http://localhost:8080}
for s in $(curl -s "$API/api/stations" | python3 -c 'import sys,json; print(" ".join(x["station_id"] for x in json.load(sys.stdin)))'); do
  t=$((600 + RANDOM % 800))
  echo "-> $s target=$t"
  curl -s -X POST "$API/api/stations/$s/recipe" -H 'Content-Type: application/json' -d "{\"target_cycle_ms\": $t}" >/dev/null
done
sleep 2
echo "-> injecting fault on station-3"
curl -s -X POST "$API/api/stations/station-3/chaos" >/dev/null
echo "watch: http://localhost:8080"
