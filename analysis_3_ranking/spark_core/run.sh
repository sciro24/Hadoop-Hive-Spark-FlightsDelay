#!/bin/bash
# ─── Analisi 3.3 — Spark Core ────────────────────────────────────────────────
set -e

echo "=== Analisi 3.3 — Spark Core ==="
echo "Start: $(date)"
START=$(date +%s)

spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=8 \
    analysis_3_ranking/spark_core/job.py

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"