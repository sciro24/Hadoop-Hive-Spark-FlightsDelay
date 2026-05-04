#!/bin/bash
# ─── Analisi 3.3 — Spark SQL ─────────────────────────────────────────────────
set -e

# Usa il sample passato dal benchmark runner, altrimenti il cleaned completo
INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"

echo "=== Analisi 3.3 — Spark SQL ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=8 \
    analysis_3_ranking/spark_sql/job.py "$INPUT"   

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"