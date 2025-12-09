#!/bin/bash
set -e

echo "🧹 Tearing down test environment..."

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Stop and remove containers
echo -e "${YELLOW}Stopping Docker Compose services...${NC}"
docker-compose down -v

# Clean up test data directories
echo -e "${YELLOW}Cleaning up test data...${NC}"
rm -rf ./docker_data
rm -rf ./test_zenith_data
rm -rf ./dlq

echo -e "${GREEN}✅ Test environment cleaned up!${NC}"
