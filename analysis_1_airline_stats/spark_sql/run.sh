#!/bin/bash
# ─── Analisi 3.1 — Spark SQL ─────────────────────────────────────────────────
set -e

echo "=== Analisi 3.1 — Spark SQL ==="
echo "Start: $(date)"
START=$(date +%s)

spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=8 \
    analysis_1_airline_stats/spark_sql/job.py

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"