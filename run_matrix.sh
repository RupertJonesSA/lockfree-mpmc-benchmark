#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="build/bin"
OUT_DIR="results/raw"
mkdir -p "$OUT_DIR"

# Limited matrix: CUR_MCGRP in {1,4,7}, MAXN in {4096, 32768}, seeds 1..5
CURS=(1 4 7)
CAPS=(4096 32768)
SEEDS=$(seq 1 5)

for impl in lock lockfree; do
  for cur in "${CURS[@]}"; do
    for cap in "${CAPS[@]}"; do
      for seed in $SEEDS; do
        bin="$BIN_DIR/bench_$impl"
        outfile="$OUT_DIR/${impl}_cur${cur}_cap${cap}_seed${seed}.csv"
        echo "Running $bin  CUR_MCGRP=$cur  MAXN=$cap  SEED=$seed"
        CXXFLAGS_EXTRA="-DCUR_MCGRP=$cur -DMAXN=$cap -DSEED_OVERRIDE=$seed"
        # Recompile quickly with overrides (fast relink since small project)
         g++ -std=c++20 -Wall -Wextra -O3 -DNDEBUG -march=native -pthread \
            -Iinclude src/packet-benchmark/main.cpp common/fix.cpp \
            $([ "$impl" = "lock" ] && echo -DUSE_LOCK) \
            -DCUR_MCGRP=$cur -DMAXN=$cap -DSEED=$seed \
            -o "$bin"

         printf '%s\n' "$outfile" | "$bin"
      done
    done
  done
done

echo "Done. Raw outputs in $OUT_DIR"
