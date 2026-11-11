#!/usr/bin/env bash
# Build images, create a kind cluster, load images, and helm install.
set -euo pipefail
cd "$(dirname "$0")/.."

TAG=${TAG:-dev}
command -v kind >/dev/null || { echo "install kind: brew install kind"; exit 1; }
command -v helm >/dev/null || { echo "install helm: brew install helm"; exit 1; }

echo "==> building images"
for svc in plc-sim gateway processor api; do
  docker build --target "$svc" -t "andon-$svc:$TAG" .
done

if ! kind get clusters | grep -qx andon; then
  echo "==> creating kind cluster"
  kind create cluster --config deploy/kind/kind-config.yaml
fi

echo "==> loading images"
for svc in plc-sim gateway processor api; do
  kind load docker-image "andon-$svc:$TAG" --name andon
done

echo "==> copying grafana dashboard into chart"
mkdir -p deploy/helm/andon/dashboards
cp deploy/grafana/dashboards/andon-line.json deploy/helm/andon/dashboards/

echo "==> helm install"
helm upgrade --install andon deploy/helm/andon -n andon --create-namespace --set image.tag="$TAG" --wait --timeout 5m

kubectl -n andon get pods
echo
echo "Dashboard: http://localhost:30080   Grafana: http://localhost:30300"
