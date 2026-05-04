#!/bin/bash
# ─── Analisi 3.2 — Spark SQL ─────────────────────────────────────────────────
set -e

echo "=== Analisi 3.2 — Spark SQL ==="
echo "Start: $(date)"
START=$(date +%s)

spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=8 \
    analysis_2_delay_report/spark_sql/job.py

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"