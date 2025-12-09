#!/bin/bash
set -e

echo "🚀 Setting up test environment..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Start Docker Compose
echo -e "${YELLOW}Starting Docker Compose services...${NC}"
docker-compose up -d

# Wait for PostgreSQL to be healthy
echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"
timeout 60 bash -c 'until docker exec zenith-postgres pg_isready -U user -d mydb > /dev/null 2>&1; do sleep 1; done'
echo -e "${GREEN}✓ PostgreSQL is ready${NC}"

# Wait for ClickHouse to be healthy
echo -e "${YELLOW}Waiting for ClickHouse to be ready...${NC}"
timeout 60 bash -c 'until curl -s http://localhost:8123/ping > /dev/null 2>&1; do sleep 1; done'
echo -e "${GREEN}✓ ClickHouse is ready${NC}"

# Setup PostgreSQL for CDC
echo -e "${YELLOW}Configuring PostgreSQL for CDC...${NC}"
docker exec zenith-postgres psql -U user -d mydb <<-EOSQL
    -- Create publication if not exists
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'zenith_pub') THEN
            CREATE PUBLICATION zenith_pub FOR ALL TABLES;
        END IF;
    END
    \$\$;
    
    -- Grant replication permissions
    ALTER USER user WITH REPLICATION;
EOSQL

echo -e "${GREEN}✓ PostgreSQL configured for CDC${NC}"

# Create test tables in PostgreSQL
echo -e "${YELLOW}Creating test tables...${NC}"
docker exec zenith-postgres psql -U user -d mydb <<-EOSQL
    -- Drop existing test tables
    DROP TABLE IF EXISTS test_users CASCADE;
    DROP TABLE IF EXISTS test_orders CASCADE;
    DROP TABLE IF EXISTS test_large_table CASCADE;
    
    -- Create test tables
    CREATE TABLE test_users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE test_orders (
        order_id SERIAL PRIMARY KEY,
        user_id INTEGER,
        amount DECIMAL(10,2),
        status TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE test_large_table (
        id SERIAL PRIMARY KEY,
        data TEXT,
        value INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Insert initial test data
    INSERT INTO test_users (name, email) VALUES 
        ('Alice', 'alice@example.com'),
        ('Bob', 'bob@example.com'),
        ('Charlie', 'charlie@example.com');
    
    INSERT INTO test_orders (user_id, amount, status) VALUES 
        (1, 99.99, 'completed'),
        (2, 149.99, 'pending'),
        (1, 49.99, 'completed');
EOSQL

echo -e "${GREEN}✓ Test tables created${NC}"

# Verify ClickHouse is accessible
echo -e "${YELLOW}Verifying ClickHouse connection...${NC}"
curl -s "http://localhost:8123/?query=SELECT%201" > /dev/null
echo -e "${GREEN}✓ ClickHouse is accessible${NC}"

echo -e "${GREEN}✅ Test environment is ready!${NC}"
echo ""
echo "PostgreSQL: postgres://user:password@localhost:5432/mydb"
echo "ClickHouse: http://localhost:8123"
echo ""
echo "Run tests with: cargo test --test e2e -- --ignored"
