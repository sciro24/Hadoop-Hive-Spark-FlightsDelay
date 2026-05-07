#!/bin/bash
# ─── Analisi 3.3 — MapReduce (Hadoop Streaming) ──────────────────────────────
set -e

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

MAPPER="analysis_3_ranking/mapreduce/mapper.py"
REDUCER="analysis_3_ranking/mapreduce/reducer.py"
STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming-*.jar" 2>/dev/null | head -1)
# Fallback per EMR dove il jar non ha il numero di versione nel nome
if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find /usr/lib/hadoop-mapreduce -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
fi
if [ -z "$STREAMING_JAR" ]; then
    echo "❌ hadoop-streaming jar non trovato. Imposta HADOOP_HOME correttamente." >&2
    exit 1
fi

echo "=== Analisi 3.3 — MapReduce ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

if [ "${CLUSTER_MODE:-false}" = "true" ]; then
    # ── Modalità cluster AWS EMR ──────────────────────────────────────────────
    S3_BUCKET="${S3_BUCKET:?Imposta la variabile S3_BUCKET}"
    S3_PREFIX="${S3_PREFIX:-flights-delay}"

    MR_OUTPUT="s3://${S3_BUCKET}/${S3_PREFIX}/results/analysis_3/mapreduce/$(date +%s)"

    echo "Output MR: $MR_OUTPUT"

    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.reduces="${NUM_REDUCERS:-4}" \
        -input  "$INPUT" \
        -output "$MR_OUTPUT" \
        -mapper  "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -file    "$MAPPER" \
        -file    "$REDUCER"

    echo "Output scritto in: $MR_OUTPUT"

else
    # ── Modalità locale ───────────────────────────────────────────────────────
    HDFS_INPUT="/user/mapreduce/analysis_3/input"
    HDFS_OUTPUT="/user/mapreduce/analysis_3/output"
    OUTPUT_LOCAL="results/analysis_3/mapreduce"

    mkdir -p "$OUTPUT_LOCAL"

    echo "Caricamento CSV su HDFS..."
    hadoop fs -mkdir -p "$HDFS_INPUT"
    hadoop fs -put -f "$INPUT" "$HDFS_INPUT/"

    hadoop fs -rm -r -f "$HDFS_OUTPUT"

    echo "Lancio job MapReduce..."
    hadoop jar "$STREAMING_JAR" \
        -input   "$HDFS_INPUT/$INPUT_FILENAME" \
        -output  "$HDFS_OUTPUT" \
        -mapper  "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -file    "$MAPPER" \
        -file    "$REDUCER"

    if [[ "$INPUT" == *"cleaned"* ]]; then
        echo "Dataset completo rilevato. Aggiornamento risultati finali..."
        echo "origin|carrier|num_flights|avg_dep_delay|avg_arr_delay|cancel_rate|avg_dep_airport|dep_diff|rank" > "$OUTPUT_LOCAL/output.csv"
        hadoop fs -cat "$HDFS_OUTPUT/part-*" | sed 's/\t/|/g; s/[|[:space:]]*$//' >> "$OUTPUT_LOCAL/output.csv"
    else
        echo "Dataset sample rilevato ($INPUT_FILENAME). Salto aggiornamento output.csv."
    fi
fi

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
if [ "${CLUSTER_MODE:-false}" != "true" ] && [ -f "results/analysis_3/mapreduce/output.csv" ]; then
    head -n 10 "results/analysis_3/mapreduce/output.csv"
fi
