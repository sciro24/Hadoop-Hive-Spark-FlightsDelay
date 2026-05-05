#!/bin/bash
# ─── Analisi 3.1 — Hive ──────────────────────────────────────────────────────
set -e

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

HDFS_DIR="/user/hive/warehouse/flights_clean"
OUTPUT_DIR="results/analysis_1/hive"
HQL="analysis_1_airline_stats/hive/queries.hql"

echo "=== Analisi 3.1 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# ─── Carica CSV su HDFS ───────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_DIR"
hadoop fs -put -f "$INPUT" "$HDFS_DIR/"

# ─── Ricrea la tabella Hive ───────────────────────────────────────────────────
hive -e "
USE flights;
DROP TABLE IF EXISTS flights_clean;
CREATE EXTERNAL TABLE flights_clean (
    fl_date STRING, year INT, month INT,
    op_unique_carrier STRING, origin STRING, dest STRING,
    dep_delay DOUBLE, arr_delay DOUBLE,
    cancelled DOUBLE, cancellation_code STRING,
    carrier_delay DOUBLE, weather_delay DOUBLE, nas_delay DOUBLE,
    security_delay DOUBLE, late_aircraft_delay DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '$HDFS_DIR'
TBLPROPERTIES ('skip.header.line.count'='1');
" 2>/dev/null

# ─── Esegui HQL ──────────────────────────────────────────────────────────────
echo "Esecuzione query Hive..."
hive -f "$HQL" 2>&1 | tee "$OUTPUT_DIR/hive_log.txt"

# ─── Esporta risultato ────────────────────────────────────────────────────────
echo "Esportazione risultati..."
hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY '$OUTPUT_DIR/raw'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT
    carrier,
    origin,
    month,
    num_flights,
    min_arr_delay,
    max_arr_delay,
    avg_arr_delay,
    cancel_rate,
    months_active
FROM results_airline_stats
ORDER BY carrier, origin, month;
" 2>/dev/null

# Aggiungi header e unisci i part file
echo "carrier|origin|month|num_flights|min_arr_delay|max_arr_delay|avg_arr_delay|cancel_rate|months_active" \
    > "$OUTPUT_DIR/output.csv"

cat "$OUTPUT_DIR"/raw/000000_0 >> "$OUTPUT_DIR/output.csv" 2>/dev/null || \
cat "$OUTPUT_DIR"/raw/* >> "$OUTPUT_DIR/output.csv"

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT_DIR/output.csv"