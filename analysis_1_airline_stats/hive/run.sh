#!/bin/bash
# ─── Analisi 3.1 — Hive ──────────────────────────────────────────────────────
set -e
set -o pipefail

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

HDFS_DIR="/user/hive/warehouse/flights_clean"
OUTPUT_DIR="$(pwd)/results/analysis_1/hive"
HQL="analysis_1_airline_stats/hive/queries.hql"

echo "=== Analisi 3.1 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# ─── Carica Parquet su HDFS ───────────────────────────────────────────────────
echo "Caricamento Parquet su HDFS..."
hadoop fs -rm -r "$HDFS_DIR" 2>/dev/null || true
hadoop fs -put -f "$INPUT" "$HDFS_DIR" 2>/dev/null

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
STORED AS PARQUET
LOCATION '${HDFS_DIR}';
"

# ─── Esegui HQL ──────────────────────────────────────────────────────────────
echo "Esecuzione query Hive..."
hive -f "$HQL" 2>&1 \
    | grep -v -E "^SLF4J:|^WARNING:|^Logging initialized|^\s+at |AlreadyExistsException|\
^WARN |^INFO |^Number of reduce|^In order to|^set mapreduce|\
^Starting Job|^Kill Command|^Hadoop job information|Stage-[0-9]+ map =|\
^Launched|^Ended Job|^Stage-Stage|^Total MapReduce|^MapReduce Jobs|\
^MapredLocal|Execution completed|task succeeded|^Moving data|\
Dump the side|Uploaded [0-9]|End of local task|^Stage-[0-9]+ is" \
    | tee "$OUTPUT_DIR/hive_log.txt"

# ─── Pulizia output precedente ────────────────────────────────────────────────
rm -f  "$OUTPUT_DIR/output.csv"
rm -rf "$OUTPUT_DIR/raw"     # ← Hive richiede che NON esista già

# ─── Esporta risultato ────────────────────────────────────────────────────────
echo "Esportazione risultati..."
RAW_DIR="${OUTPUT_DIR}/raw"

hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY 'file://${RAW_DIR}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
COLLECTION ITEMS TERMINATED BY ','
SELECT
    carrier, origin, month, num_flights,
    min_arr_delay, max_arr_delay, avg_arr_delay,
    cancel_rate,
    months_active
FROM results_airline_stats
ORDER BY carrier, origin, month;
"

# Header + dati
echo "carrier|origin|month|num_flights|min_arr_delay|max_arr_delay|avg_arr_delay|cancel_rate|months_active" \
    > "$OUTPUT_DIR/output.csv"

cat "$RAW_DIR"/000000_0 >> "$OUTPUT_DIR/output.csv" 2>/dev/null || \
cat "$RAW_DIR"/* >> "$OUTPUT_DIR/output.csv"

echo "Pulizia file temporanei..."
rm -rf "$RAW_DIR" "$OUTPUT_DIR/hive_log.txt"

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
head -10 "$OUTPUT_DIR/output.csv"