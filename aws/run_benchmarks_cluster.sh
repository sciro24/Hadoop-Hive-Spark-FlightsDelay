#!/bin/bash
# ─── Esegui benchmark completo sul cluster EMR ───────────────────────────────
# Questo script va eseguito SUL MASTER NODE del cluster EMR dopo aver fatto
# SSH (non eseguirlo dal tuo Mac).
#
# Prima di eseguire:
#   1. Copia il progetto sul master: aws s3 sync s3://BUCKET/PREFIX/scripts/ ~/project/
#      oppure usa il comando di setup mostrato da create_cluster.sh
#   2. cd ~/project
#   3. bash aws/run_benchmarks_cluster.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# ─── Carica configurazione ────────────────────────────────────────────────────
source aws/config.sh

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║      BENCHMARK CLUSTER — Flight Delay 2024              ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  S3 Bucket:  s3://${S3_BUCKET}/${S3_PREFIX}"
echo "║  Executors:  ${NUM_EXECUTORS} x ${EXECUTOR_CORES} core x ${EXECUTOR_MEMORY}"
echo "║  Shuffles:   ${SHUFFLE_PARTITIONS} partitions"
echo "║  Reducers MR:${NUM_REDUCERS}"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─── Installa dipendenze Python se necessario ─────────────────────────────────
pip3 install --quiet pyarrow pandas 2>/dev/null || true

# ─── Crea database Hive ───────────────────────────────────────────────────────
hive -e "CREATE DATABASE IF NOT EXISTS flights;" 2>/dev/null || true

# ─── Lancia i benchmark (riusa run_benchmarks.sh con CLUSTER_MODE=true) ───────
bash benchmarks/run_benchmarks.sh

# ─── Download risultati su S3 ────────────────────────────────────────────────
echo ""
echo "► Upload results_cluster.csv su S3..."
aws s3 cp benchmarks/results_cluster.csv \
    "s3://${S3_BUCKET}/${S3_PREFIX}/benchmarks/results_cluster.csv"

echo ""
echo "✅  Benchmark cluster completati."
echo "    Risultati in: s3://${S3_BUCKET}/${S3_PREFIX}/benchmarks/results_cluster.csv"
