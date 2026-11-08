GO  ?= go
BIN := bin/carpincho

.PHONY: build run test race vet fmt check fuzz bench bench-docker docker clean

build: ## compile the server into bin/carpincho
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/carpincho

run: ## run in memory on port 6380
	$(GO) run ./cmd/carpincho --port 6380

test: ## unit + integration tests
	$(GO) test -count=1 ./...

race: ## the same tests with the race detector (needs cgo; works on Linux/macOS)
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt: ## fail if any file is not gofmt-ed
	@test -z "$$(gofmt -l .)" || (gofmt -d . && exit 1)

check: fmt vet race ## what CI runs

fuzz: ## 30 seconds on each fuzz target of the RESP parser
	$(GO) test ./internal/resp -run '^$$' -fuzz '^FuzzReadCommand$$' -fuzztime 30s
	$(GO) test ./internal/resp -run '^$$' -fuzz '^FuzzReadValue$$' -fuzztime 30s
	$(GO) test ./internal/resp -run '^$$' -fuzz '^FuzzSplitInline$$' -fuzztime 30s

bench: ## Go benchmarks (parser, sharding, command layer, TCP)
	$(GO) test -run '^$$' -bench . -benchmem ./...

bench-docker: ## carpincho vs Redis 7 with redis-benchmark (needs Docker)
	bash bench/run-docker-bench.sh

docker: ## build the image
	docker build -t carpincho .

clean:
	rm -rf bin
