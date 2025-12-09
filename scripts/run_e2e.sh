#!/bin/bash
set -e

echo "Starting E2E Environment..."
docker-compose up -d --wait

echo "Running E2E Integration Test..."
# Run the ignored test, showing output
cargo test --package zenith-core --test e2e -- --ignored --nocapture

echo "Test Complete. Tearing down..."
docker-compose down
