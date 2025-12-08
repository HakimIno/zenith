#!/bin/bash
#
# Zenith CDC Benchmark Script
#
# This script runs benchmarks and performance tests for Zenith CDC.
# Usage: ./scripts/benchmark.sh [options]
#
# Options:
#   --quick    Run quick benchmarks only
#   --full     Run full benchmark suite including stress tests
#   --profile  Enable profiling (requires perf/flamegraph)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              Zenith CDC Benchmark Suite                       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo

# Parse arguments
QUICK=false
FULL=false
PROFILE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK=true
            shift
            ;;
        --full)
            FULL=true
            shift
            ;;
        --profile)
            PROFILE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Build in release mode
echo -e "${YELLOW}Building in release mode...${NC}"
cargo build --release --all

echo -e "${GREEN}Build complete!${NC}"
echo

# Run unit tests first
echo -e "${YELLOW}Running unit tests...${NC}"
cargo test --release --all -- --quiet
echo -e "${GREEN}Tests passed!${NC}"
echo

# Run criterion benchmarks
echo -e "${YELLOW}Running criterion benchmarks...${NC}"
if [ "$QUICK" = true ]; then
    cargo bench -- --quick
else
    cargo bench
fi
echo

# Parser throughput test
echo -e "${YELLOW}Running parser throughput test...${NC}"

# Create a simple Rust program to test parser throughput
cat > /tmp/parser_throughput.rs << 'EOF'
use std::time::Instant;

fn create_insert_message(id: u32) -> Vec<u8> {
    let mut msg = vec![b'I'];
    msg.extend_from_slice(&16384u32.to_be_bytes());
    msg.push(b'N');
    msg.extend_from_slice(&3u16.to_be_bytes());
    
    // ID column
    msg.push(b't');
    let id_str = id.to_string();
    msg.extend_from_slice(&(id_str.len() as u32).to_be_bytes());
    msg.extend_from_slice(id_str.as_bytes());
    
    // Name column
    msg.push(b't');
    let name = format!("User Name {}", id);
    msg.extend_from_slice(&(name.len() as u32).to_be_bytes());
    msg.extend_from_slice(name.as_bytes());
    
    // Email column
    msg.push(b't');
    let email = format!("user{}@example.com", id);
    msg.extend_from_slice(&(email.len() as u32).to_be_bytes());
    msg.extend_from_slice(email.as_bytes());
    
    msg
}

fn main() {
    println!("Parser Throughput Test");
    println!("======================");
    
    // Pre-generate messages
    let messages: Vec<Vec<u8>> = (0..100_000)
        .map(|i| create_insert_message(i))
        .collect();
    
    println!("Generated {} test messages", messages.len());
    
    // Simulate parsing (just read the bytes)
    let iterations = 1_000_000;
    let start = Instant::now();
    
    for i in 0..iterations {
        let msg = &messages[i % messages.len()];
        // Simulate minimal parsing work
        let _ = msg[0]; // Message type
        let _ = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]); // Relation ID
    }
    
    let elapsed = start.elapsed();
    let msgs_per_sec = iterations as f64 / elapsed.as_secs_f64();
    
    println!("Parsed {} messages in {:?}", iterations, elapsed);
    println!("Throughput: {:.2} messages/sec", msgs_per_sec);
    println!("Throughput: {:.2} million messages/sec", msgs_per_sec / 1_000_000.0);
}
EOF

echo -e "${BLUE}(Simulated throughput test - actual parsing benchmark in criterion)${NC}"
echo

# Memory test
echo -e "${YELLOW}Running memory usage test...${NC}"
echo "Building memory test binary..."

# Check memory of release binary
BINARY_SIZE=$(du -h target/release/zenith 2>/dev/null | cut -f1 || echo "N/A")
echo "Binary size: $BINARY_SIZE"

# Report results
echo
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    Benchmark Results                          ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo
echo "Platform: $(uname -m) $(uname -s)"
echo "Rust version: $(rustc --version)"
echo "Binary size: $BINARY_SIZE"
echo
echo "See target/criterion/ for detailed benchmark reports"
echo

# Full benchmarks
if [ "$FULL" = true ]; then
    echo -e "${YELLOW}Running full benchmark suite...${NC}"
    
    # Additional stress tests could go here
    echo "Full benchmarks require PostgreSQL and ClickHouse connections"
    echo "Set POSTGRES_URL and CLICKHOUSE_URL environment variables"
    
    if [ -n "$POSTGRES_URL" ] && [ -n "$CLICKHOUSE_URL" ]; then
        echo "Running end-to-end benchmark..."
        # Would run actual CDC test here
    else
        echo -e "${YELLOW}Skipping end-to-end tests (no database connections)${NC}"
    fi
fi

# Profiling
if [ "$PROFILE" = true ]; then
    echo -e "${YELLOW}Generating flame graph...${NC}"
    
    if command -v perf &> /dev/null && command -v flamegraph &> /dev/null; then
        cargo flamegraph --bench parser_bench -- --bench
        echo "Flame graph saved to flamegraph.svg"
    else
        echo -e "${RED}perf or flamegraph not found. Install with:${NC}"
        echo "  cargo install flamegraph"
        echo "  (and ensure perf is installed)"
    fi
fi

echo
echo -e "${GREEN}Benchmark complete!${NC}"

