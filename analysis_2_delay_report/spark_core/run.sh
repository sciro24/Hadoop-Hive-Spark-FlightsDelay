#!/bin/bash
# ─── Analisi 3.2 — Spark Core ────────────────────────────────────────────────
set -e

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.parquet}"

echo "=== Analisi 3.2 — Spark Core ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

if [ "${CLUSTER_MODE:-false}" = "true" ]; then
    spark-submit \
        --deploy-mode client \
        --executor-memory "${EXECUTOR_MEMORY:-4g}" \
        --executor-cores  "${EXECUTOR_CORES:-2}" \
        --num-executors   "${NUM_EXECUTORS:-4}" \
        analysis_2_delay_report/spark_core/job.py "$INPUT"
else
    spark-submit \
        --master "local[*]" \
        --driver-memory 4g \
        --conf spark.sql.shuffle.partitions=8 \
        analysis_2_delay_report/spark_core/job.py "$INPUT"
fi

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"
