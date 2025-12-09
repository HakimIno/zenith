#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "🧪 Running Integration Tests..."

# Trap to ensure cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    ./scripts/teardown-test-env.sh
}
trap cleanup EXIT

# Setup environment
./scripts/setup-test-env.sh

# Build the project
echo -e "${YELLOW}Building project...${NC}"
cargo build --release

# Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"
if cargo test --test e2e -- --ignored --test-threads=1 --nocapture; then
    echo -e "${GREEN}✅ All integration tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Integration tests failed!${NC}"
    exit 1
fi
