#!/bin/bash
# ─── Analisi 3.3 — MapReduce (Hadoop Streaming) ──────────────────────────────
set -e

INPUT_LOCAL="data/cleaned/flight_data_2024_cleaned.csv"
HDFS_INPUT="/user/mapreduce/analysis_3/input"
HDFS_OUTPUT="/user/mapreduce/analysis_3/output"
OUTPUT_LOCAL="results/analysis_3/mapreduce"
MAPPER="analysis_3_ranking/mapreduce/mapper.py"
REDUCER="analysis_3_ranking/mapreduce/reducer.py"

STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming-*.jar" | head -1)

echo "=== Analisi 3.3 — MapReduce ==="
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_LOCAL"

echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_INPUT"
hadoop fs -put -f "$INPUT_LOCAL" "$HDFS_INPUT/"

hadoop fs -rm -r -f "$HDFS_OUTPUT"

echo "Lancio job MapReduce..."
hadoop jar "$STREAMING_JAR" \
    -input   "$HDFS_INPUT/flight_data_2024_cleaned.csv" \
    -output  "$HDFS_OUTPUT" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file    "$MAPPER" \
    -file    "$REDUCER"

echo "Download risultati..."
hadoop fs -getmerge "$HDFS_OUTPUT" "$OUTPUT_LOCAL/output.csv"

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
echo "origin|carrier|num_flights|avg_dep|avg_arr|cancel_rate|avg_dep_airport|dep_diff|rank"
head -10 "$OUTPUT_LOCAL/output.csv"