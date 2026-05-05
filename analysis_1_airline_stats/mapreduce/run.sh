#!/bin/bash
# ─── Analisi 3.1 — MapReduce (Hadoop Streaming) ──────────────────────────────
set -e

# Usa il sample passato dal benchmark runner, altrimenti il cleaned completo
INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")   # ← nome file dinamico

HDFS_INPUT="/user/mapreduce/analysis_1/input"
HDFS_OUTPUT="/user/mapreduce/analysis_1/output"
OUTPUT_LOCAL="results/analysis_1/mapreduce"
MAPPER="analysis_1_airline_stats/mapreduce/mapper.py"
REDUCER="analysis_1_airline_stats/mapreduce/reducer.py"

STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming-*.jar" | head -1)

echo "=== Analisi 3.1 — MapReduce ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_LOCAL"

# ─── Carica input su HDFS ────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_INPUT"
hadoop fs -put -f "$INPUT" "$HDFS_INPUT/"

# ─── Rimuovi output HDFS precedente ──────────────────────────────────────────
hadoop fs -rm -r -f "$HDFS_OUTPUT"

# ─── Lancia job Hadoop Streaming ─────────────────────────────────────────────
echo "Lancio job MapReduce..."
hadoop jar "$STREAMING_JAR" \
    -input   "$HDFS_INPUT/$INPUT_FILENAME" \
    -output  "$HDFS_OUTPUT" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file    "$MAPPER" \
    -file    "$REDUCER"

# ─── Scarica output da HDFS ──────────────────────────────────────────────────
echo "Download risultati..."
echo "carrier|origin|month|num_flights|min_arr_delay|max_arr_delay|avg_arr_delay|cancel_rate|months_active" > "$OUTPUT_LOCAL/output.csv"
hadoop fs -cat "$HDFS_OUTPUT/part-*" >> "$OUTPUT_LOCAL/output.csv"

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT_LOCAL/output.csv"