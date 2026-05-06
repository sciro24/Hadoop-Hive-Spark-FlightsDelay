#!/bin/bash
# ─── Analisi 3.2 — Hive ──────────────────────────────────────────────────────
set -e

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

HDFS_DIR="/user/hive/warehouse/flights_clean"
OUTPUT_DIR="$(pwd)/results/analysis_2/hive"
HQL="analysis_2_delay_report/hive/queries.hql"

echo "=== Analisi 3.2 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# ─── Carica CSV su HDFS ──────────────────────────────────────────────────────
echo "Caricamento CSV su HDFS..."
hadoop fs -mkdir -p "$HDFS_DIR" 2>/dev/null
hadoop fs -put -f "$INPUT" "$HDFS_DIR/" 2>/dev/null

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
LOCATION '${HDFS_DIR}'
TBLPROPERTIES ('skip.header.line.count'='1');
" 2>/dev/null

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
rm -f  "$OUTPUT_DIR/output_delay_report.csv"
rm -f  "$OUTPUT_DIR/output_delay_causes.csv"
rm -rf "$OUTPUT_DIR/delay_report" "$OUTPUT_DIR/delay_causes"   # NON ricreare con mkdir

# ─── Esporta risultati ────────────────────────────────────────────────────────
echo "Esportazione risultati..."

DELAY_REPORT_DIR="${OUTPUT_DIR}/delay_report"
DELAY_CAUSES_DIR="${OUTPUT_DIR}/delay_causes"

hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY 'file://${DELAY_REPORT_DIR}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT * FROM results_delay_report ORDER BY origin, month, delay_band;
"

hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY 'file://${DELAY_CAUSES_DIR}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT * FROM results_delay_causes ORDER BY origin, month, rank_pos;
"

echo "origin|month|delay_band|num_flights|avg_dep_delay|avg_arr_delay" \
    > "$OUTPUT_DIR/output_delay_report.csv"
cat "$DELAY_REPORT_DIR"/* >> "$OUTPUT_DIR/output_delay_report.csv" 2>/dev/null || true

echo "origin|month|cause|avg_minutes|rank_pos" \
    > "$OUTPUT_DIR/output_delay_causes.csv"
cat "$DELAY_CAUSES_DIR"/* >> "$OUTPUT_DIR/output_delay_causes.csv" 2>/dev/null || true

echo "Pulizia file temporanei..."
rm -rf "$DELAY_REPORT_DIR" "$DELAY_CAUSES_DIR" "$OUTPUT_DIR/hive_log.txt"

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe delay_report ==="
head -10 "$OUTPUT_DIR/output_delay_report.csv"
echo ""
echo "=== Prime 10 righe delay_causes ==="
head -10 "$OUTPUT_DIR/output_delay_causes.csv"