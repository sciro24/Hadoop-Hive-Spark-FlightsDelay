#!/bin/bash
# ─── Analisi 3.1 — Hive ──────────────────────────────────────────────────────
set -e

CLEANED_CSV="data/cleaned/flight_data_2024_cleaned.csv"
HDFS_DIR="/user/hive/warehouse/flights_clean"
OUTPUT_DIR="results/analysis_1/hive"
HQL="analysis_1_airline_stats/hive/queries.hql"

echo "=== Analisi 3.1 — Hive ==="
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# ─── Carica CSV su HDFS ──────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_DIR"
hadoop fs -put -f "$CLEANED_CSV" "$HDFS_DIR/"

# ─── Esegui HQL ──────────────────────────────────────────────────────────────
echo "Esecuzione query Hive..."
hive -f "$HQL" 2>&1 | tee "$OUTPUT_DIR/hive_log.txt"

# ─── Esporta risultato da Hive a file locale ─────────────────────────────────
echo "Esportazione risultati..."
hive -e "USE flights; INSERT OVERWRITE LOCAL DIRECTORY '$OUTPUT_DIR/raw'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT carrier, origin, month, num_flights,
       min_arr_delay, max_arr_delay, avg_arr_delay,
       cancel_rate, active_months
FROM results_airline_stats
ORDER BY carrier, origin, month;" 2>/dev/null

# Merge dei file part-* in un unico output
cat "$OUTPUT_DIR"/raw/000000_0 > "$OUTPUT_DIR/output.csv" 2>/dev/null || \
cat "$OUTPUT_DIR"/raw/* > "$OUTPUT_DIR/output.csv"

END=$(date +%s)
ELAPSED=$((END - START))
echo "End: $(date)"
echo "Tempo di esecuzione: ${ELAPSED}s"

echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT_DIR/output.csv"