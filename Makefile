.PHONY: help build test test-unit test-integration clean docker-up docker-down

help:
	@echo "Zenith CDC - Makefile Commands"
	@echo ""
	@echo "  make build              - Build release binary"
	@echo "  make test               - Run all tests"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make docker-up          - Start Docker services"
	@echo "  make docker-down        - Stop Docker services"
	@echo "  make clean              - Clean build artifacts"

build:
	cargo build --release

test: test-unit test-integration

test-unit:
	cargo test --lib

test-integration:
	@echo "Running integration tests..."
	@./scripts/run-integration-tests.sh

docker-up:
	docker-compose up -d
	@echo "Waiting for services..."
	@sleep 5

docker-down:
	docker-compose down -v

clean:
	cargo clean
	rm -rf ./docker_data
	rm -rf ./test_zenith_data
	rm -rf ./dlq
