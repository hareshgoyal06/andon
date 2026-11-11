.PHONY: build test lint web up down logs ps restart-processor failover kind-up kind-down clean

## --- local dev (no docker) -------------------------------------------------
build:            ## build all go binaries into bin/
	go build -o bin/ ./cmd/...

test:             ## run unit tests with the race detector
	go test -race ./...

lint:             ## vet + gofmt check
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt -w ." && exit 1)

web:              ## build the dashboard into web/dist
	cd web && npm ci && npm run build

run-plc:          ## run 6 simulated PLCs (no kafka needed)
	go run ./cmd/plc-sim -stations 6 -base-port 5020

run-gateway-stdout: ## poll PLCs and print events to stdout (no kafka needed)
	go run ./cmd/gateway -stations localhost:5020,localhost:5021,localhost:5022,localhost:5023,localhost:5024,localhost:5025

## --- docker compose -------------------------------------------------------
up:               ## build + start the full stack
	docker compose up -d --build

down:             ## stop and remove everything (incl. data)
	docker compose down -v

logs:             ## tail logs for the go services
	docker compose logs -f gateway processor api

ps:
	docker compose ps

restart-processor: ## rolling restart of processors (zero-downtime check)
	docker compose restart processor

failover:         ## SIGKILL one processor and verify no data loss
	./scripts/failover-demo.sh

chaos:            ## push random recipes + inject a fault
	./scripts/load-test.sh

## --- kubernetes (kind) ----------------------------------------------------
kind-up:          ## build images, create kind cluster, helm install
	./scripts/kind-up.sh

kind-down:
	kind delete cluster --name andon

helm-lint:
	helm lint deploy/helm/andon

clean:
	rm -rf bin web/dist

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
