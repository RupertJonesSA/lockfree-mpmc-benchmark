#!/usr/bin/env bash
set -euo pipefail

# Build two binaries with minimal code changes:
# - bench_lock (uses mpmc_lock_ring) 
# - bench_lockfree (uses mpmc_ring)
#
# You can override SIMULATION_TIME_S, CUR_MCGRP, MAXN, MAX_RX via -D flags.

CXX=${CXX:-g++}
CXXFLAGS="-std=c++20 -Wall -Wextra -O3 -DNDEBUG -march=native -pthread"

SRC_DIR="src/packet-benchmark"
INC_DIR="include"
COMMON="common/fix.cpp"

mkdir -p build/bin

# Compile lock-free
$CXX $CXXFLAGS -I"$INC_DIR" "$SRC_DIR/main.cpp" "$COMMON" -o build/bin/bench_lockfree

# Compile lock (with -DUSE_LOCK)
$CXX $CXXFLAGS -DUSE_LOCK -I"$INC_DIR" "$SRC_DIR/main.cpp" "$COMMON" -o build/bin/bench_lock

echo "Built: build/bin/bench_lockfree, build/bin/bench_lock"
