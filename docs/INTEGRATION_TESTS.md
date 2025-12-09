# Integration Tests Guide

## Overview

Comprehensive end-to-end integration tests for Zenith CDC that verify the entire pipeline works correctly in real-world scenarios.

## Test Coverage

### 1. **Basic Snapshot & Streaming** (`test_basic_snapshot_and_streaming`)

- Verifies initial snapshot functionality
- Tests streaming inserts/updates
- Validates data consistency

### 2. **Parallel Snapshot** (`test_parallel_snapshot`)

- Creates 5 tables with 10K rows each
- Measures parallel snapshot performance
- Verifies concurrent table processing
- **Expected**: ~50K rows in <30 seconds

### 3. **Row-level Resumability** (`test_resumability`)

- Creates 50K row table
- Kills process mid-snapshot
- Restarts and verifies resume from checkpoint
- Validates no duplicate data
- **Key Feature**: Tests PK-based chunking

### 4. **Dead Letter Queue** (`test_dead_letter_queue`)

- Simulates schema mismatch errors
- Verifies DLQ file creation
- Validates pipeline continues despite errors
- Checks error logging format

### 5. **Composite PK Resumability** (`test_composite_pk_resumability`)

- Tests resumability with composite primary keys
- Verifies `(user_id, order_id)` PK handling
- Validates WHERE clause generation

### 6. **High-Volume Stress Test** (`test_high_volume_stress`)

- Rapidly inserts 100K rows
- Measures end-to-end throughput
- Validates ordering preservation
- **Expected**: >10K events/sec

## Prerequisites

### Required Services

- Docker & Docker Compose
- PostgreSQL 15+ (via Docker)
- ClickHouse 23.8+ (via Docker)

### Build Requirements

- Rust 1.70+
- Cargo

## Quick Start

### Option 1: Using Makefile (Recommended)

```bash
# Run all integration tests
make test-integration

# Start services only
make docker-up

# Stop services
make docker-down
```

### Option 2: Manual Steps

```bash
# 1. Setup environment
./scripts/setup-test-env.sh

# 2. Build project
cargo build --release

# 3. Run tests
cargo test --test e2e -- --ignored --test-threads=1 --nocapture

# 4. Cleanup
./scripts/teardown-test-env.sh
```

### Option 3: Run Specific Test

```bash
# Setup
./scripts/setup-test-env.sh

# Run single test
cargo test --test e2e test_resumability -- --ignored --nocapture

# Cleanup
./scripts/teardown-test-env.sh
```

## Test Infrastructure

### Docker Compose Services

```yaml
services:
  postgres:
    - Port: 5432
    - User: user
    - Password: password
    - Database: mydb
    - WAL Level: logical

  clickhouse:
    - HTTP Port: 8123
    - Native Port: 9000
```

### Test Scripts

#### `scripts/setup-test-env.sh`

- Starts Docker Compose
- Waits for services to be healthy
- Creates publication (`zenith_pub`)
- Creates test tables
- Grants replication permissions

#### `scripts/teardown-test-env.sh`

- Stops Docker containers
- Removes volumes
- Cleans up test data

#### `scripts/run-integration-tests.sh`

- Full test lifecycle
- Automatic cleanup on exit
- Error handling

## Test Utilities (`tests/common/mod.rs`)

### Database Helpers

```rust
setup_postgres() -> PgClient
setup_clickhouse() -> Client
```

### Data Generation

```rust
generate_large_dataset(rows: usize) -> Vec<(String, i32)>
insert_large_dataset(client, table, data) -> Result<()>
```

### Verification

```rust
verify_clickhouse_count(client, table, expected) -> Result<()>
wait_for_replication(client, table, count, timeout) -> Result<()>
```

### Process Management

```rust
start_zenith(args: &[&str]) -> Child
kill_zenith(child: Child) -> Result<()>
```

## Expected Results

### Performance Benchmarks

| Test              | Expected Duration | Throughput      |
| ----------------- | ----------------- | --------------- |
| Basic             | <10s              | N/A             |
| Parallel Snapshot | <30s              | ~2K rows/sec    |
| Resumability      | <60s              | ~1K rows/sec    |
| DLQ               | <15s              | N/A             |
| Composite PK      | <45s              | ~500 rows/sec   |
| Stress Test       | <120s             | >10K events/sec |

### Success Criteria

- ✅ All tests pass
- ✅ No data loss
- ✅ No duplicate data
- ✅ DLQ captures errors
- ✅ Resumability works correctly
- ✅ Performance meets benchmarks

## Troubleshooting

### Tests Fail to Start

```bash
# Check Docker services
docker-compose ps

# Check logs
docker-compose logs postgres
docker-compose logs clickhouse

# Restart services
make docker-down
make docker-up
```

### Connection Errors

```bash
# Verify PostgreSQL
docker exec zenith-postgres pg_isready -U user -d mydb

# Verify ClickHouse
curl http://localhost:8123/ping
```

### Port Conflicts

```bash
# Check port usage
lsof -i :5432
lsof -i :8123

# Stop conflicting services
docker-compose down
```

### Cleanup Issues

```bash
# Force cleanup
docker-compose down -v
rm -rf ./docker_data ./test_zenith_data ./dlq
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: make test-integration
```

## Development Workflow

### Adding New Tests

1. Add test function to `zenith-core/tests/e2e.rs`
2. Mark with `#[tokio::test]` and `#[ignore]`
3. Use utilities from `common/mod.rs`
4. Follow naming convention: `test_<feature>`
5. Add cleanup in test

### Best Practices

- Use `--test-threads=1` to avoid conflicts
- Always cleanup resources
- Use descriptive assertions
- Log progress with `println!`
- Set realistic timeouts

## Known Limitations

1. **Sequential Execution**: Tests must run with `--test-threads=1`
2. **Docker Required**: Cannot run without Docker
3. **Timing Sensitive**: Some tests depend on timing
4. **Resource Intensive**: Requires ~2GB RAM

## Future Improvements

- [ ] Parallel test execution
- [ ] Mock services for faster tests
- [ ] Test result reporting
- [ ] Performance regression detection
- [ ] Chaos engineering tests
- [ ] Multi-platform CI

## Support

For issues or questions:

1. Check logs: `docker-compose logs`
2. Verify services: `make docker-up`
3. Clean state: `make docker-down`
4. Review test output: `--nocapture`
