#!/bin/bash
# ─── Analisi 3.2 — Hive ──────────────────────────────────────────────────────
set -e

# Usa il sample passato dal benchmark runner, altrimenti il cleaned completo
INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

HDFS_DIR="/user/hive/warehouse/flights_clean"
OUTPUT_DIR="results/analysis_2/hive"
HQL="analysis_2_delay_report/hive/queries.hql"

echo "=== Analisi 3.2 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# ─── Carica CSV su HDFS ──────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_DIR"
hadoop fs -put -f "$INPUT" "$HDFS_DIR/"

# ─── Ricrea la tabella Hive puntando al sample corretto ───────────────────────
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

# ─── Esporta risultati ────────────────────────────────────────────────────────
echo "Esportazione risultati..."
hive -e "USE flights; INSERT OVERWRITE LOCAL DIRECTORY '$OUTPUT_DIR/delay_report'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT * FROM results_delay_report ORDER BY origin, month, delay_band;" 2>/dev/null

hive -e "USE flights; INSERT OVERWRITE LOCAL DIRECTORY '$OUTPUT_DIR/delay_causes'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT * FROM results_delay_causes ORDER BY origin, month, rank_pos;" 2>/dev/null

cat "$OUTPUT_DIR"/delay_report/* > "$OUTPUT_DIR/output_delay_report.csv" 2>/dev/null || true
cat "$OUTPUT_DIR"/delay_causes/*  > "$OUTPUT_DIR/output_delay_causes.csv"  2>/dev/null || true

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe delay_report ==="
head -10 "$OUTPUT_DIR/output_delay_report.csv"
echo ""
echo "=== Prime 10 righe delay_causes ==="
head -10 "$OUTPUT_DIR/output_delay_causes.csv"