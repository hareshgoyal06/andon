.PHONY: build run-plc run-gateway test lint up down

build:
	go build -o bin/ ./cmd/...

run-plc:
	go run ./cmd/plc-sim -stations 5 -base-port 5020

run-gateway:
	go run ./cmd/gateway -stations localhost:5020,localhost:5021,localhost:5022,localhost:5023,localhost:5024

test:
	go test -race ./...

lint:
	go vet ./...

up:
	docker compose up -d

down:
	docker compose down -v
