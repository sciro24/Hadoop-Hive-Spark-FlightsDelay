#!/bin/bash
# ─── Analisi 3.1 — MapReduce (Hadoop Streaming) ──────────────────────────────
set -e

INPUT_LOCAL="data/cleaned/flight_data_2024_cleaned.csv"
HDFS_INPUT="/user/mapreduce/analysis_1/input"
HDFS_OUTPUT="/user/mapreduce/analysis_1/output"
OUTPUT_LOCAL="results/analysis_1/mapreduce"
MAPPER="analysis_1_airline_stats/mapreduce/mapper.py"
REDUCER="analysis_1_airline_stats/mapreduce/reducer.py"

# Trova il jar di Hadoop Streaming
STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming-*.jar" | head -1)

echo "=== Analisi 3.1 — MapReduce ==="
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_LOCAL"

# ─── Carica input su HDFS ────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_INPUT"
hadoop fs -put -f "$INPUT_LOCAL" "$HDFS_INPUT/"

# ─── Rimuovi output HDFS precedente ─────────────────────────────────────────
hadoop fs -rm -r -f "$HDFS_OUTPUT"

# ─── Lancia job Hadoop Streaming ─────────────────────────────────────────────
echo "Lancio job MapReduce..."
hadoop jar "$STREAMING_JAR" \
    -input   "$HDFS_INPUT/flight_data_2024_cleaned.csv" \
    -output  "$HDFS_OUTPUT" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file    "$MAPPER" \
    -file    "$REDUCER"

# ─── Scarica output da HDFS ──────────────────────────────────────────────────
echo "Download risultati..."
hadoop fs -getmerge "$HDFS_OUTPUT" "$OUTPUT_LOCAL/output.csv"

END=$(date +%s)
ELAPSED=$((END - START))
echo "End: $(date)"
echo "Tempo di esecuzione: ${ELAPSED}s"

echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT_LOCAL/output.csv"