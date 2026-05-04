#!/bin/bash
# ─── Analisi 3.1 — MapReduce ─────────────────────────────────────────────────
set -e

INPUT="data/cleaned/flight_data_2024_cleaned.csv"
OUTPUT="results/analysis_1/mapreduce"
MAPPER="analysis_1_airline_stats/mapreduce/mapper.py"
REDUCER="analysis_1_airline_stats/mapreduce/reducer.py"

echo "=== Analisi 3.1 — MapReduce ==="
echo "Start: $(date)"
START=$(date +%s)

# Rimuovi output precedente
rm -rf "$OUTPUT"
mkdir -p "$OUTPUT"

# Esecuzione in locale con Hadoop Streaming
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar" \
    -input    "$INPUT" \
    -output   "$OUTPUT/raw" \
    -mapper   "python3 $MAPPER" \
    -reducer  "python3 $REDUCER" \
    -file     "$MAPPER" \
    -file     "$REDUCER"

# Salva output
hadoop fs -getmerge "$OUTPUT/raw" "$OUTPUT/output.csv"

END=$(date +%s)
ELAPSED=$((END - START))
echo "End: $(date)"
echo "Tempo di esecuzione: ${ELAPSED}s"
echo "Risultato: $OUTPUT/output.csv"

# Prime 10 righe
echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT/output.csv"