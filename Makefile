.PHONY: build test test-all clean install fmt lint bench

# CGO flags for SQLite with math functions enabled
export CGO_ENABLED=1
export CGO_CFLAGS=-DSQLITE_ENABLE_MATH_FUNCTIONS
export CGO_LDFLAGS=-lm

# Build the CLI
build:
	go build -o aul ./cmd/aul

# Install globally
install:
	go install ./cmd/aul

# Run all tests
test:
	go test -v ./...

# Alias for test
test-all: test

# Quick smoke test
test-quick:
	go test -v ./... -run "TestBasic" -count=1

# Run benchmarks
bench:
	go test -bench=. -benchmem ./runtime/...

# Run benchmarks with count for stability
bench-stable:
	go test -bench=. -benchmem -count=5 ./runtime/... | tee benchmark_results.txt

# Clean build artifacts
clean:
	rm -f aul
	rm -rf jit_cache/
	rm -f benchmark_results.txt

# Format code
fmt:
	go fmt ./...
	gofmt -s -w .

# Lint
lint:
	go vet ./...

# Run the server (development)
run: build
	./aul --http-port 8080 -d ./examples/procedures

# Run with all protocols
run-all: build
	./aul --tds-port 1433 --pg-port 5432 --http-port 8080 -d ./examples/procedures

# Generate version
version:
	@echo "aul version $$(cat VERSION)"

# Show help
help:
	@echo "aul - Multi-protocol database server"
	@echo ""
	@echo "Build & Install:"
	@echo "  make build            Build the server binary"
	@echo "  make install          Install globally via go install"
	@echo "  make clean            Remove build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  make test             Run all tests"
	@echo "  make test-quick       Quick smoke test"
	@echo "  make bench            Run benchmarks"
	@echo "  make bench-stable     Run benchmarks 5x for stable results"
	@echo ""
	@echo "Development:"
	@echo "  make run              Run server with HTTP on port 8080"
	@echo "  make run-all          Run with TDS, PostgreSQL, and HTTP"
	@echo "  make fmt              Format Go code"
	@echo "  make lint             Run go vet"
