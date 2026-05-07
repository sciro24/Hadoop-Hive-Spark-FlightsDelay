#!/bin/bash
# ─── Analisi 3.3 — Spark SQL ─────────────────────────────────────────────────
set -e

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.parquet}"

echo "=== Analisi 3.3 — Spark SQL ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

if [ "${CLUSTER_MODE:-false}" = "true" ]; then
    spark-submit \
        --deploy-mode client \
        --executor-memory "${EXECUTOR_MEMORY:-4g}" \
        --executor-cores  "${EXECUTOR_CORES:-2}" \
        --num-executors   "${NUM_EXECUTORS:-4}" \
        --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-200}" \
        analysis_3_ranking/spark_sql/job.py "$INPUT"
else
    spark-submit \
        --master "local[*]" \
        --driver-memory 4g \
        --conf spark.sql.shuffle.partitions=8 \
        analysis_3_ranking/spark_sql/job.py "$INPUT"
fi

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"
