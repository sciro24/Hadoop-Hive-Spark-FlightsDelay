#!/bin/bash
# ─── Analisi 3.2 — Hive ──────────────────────────────────────────────────────
set -e
set -o pipefail

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
rm -f  "$OUTPUT_DIR/output_delay_report.csv"
rm -f  "$OUTPUT_DIR/output_delay_causes.csv"
rm -rf "$OUTPUT_DIR/delay_report" "$OUTPUT_DIR/delay_causes"   # NON ricreare con mkdir

# ─── Esporta i risultati ──────────────────────────────────────────────────────
echo "Esportazione risultati..."
hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
SELECT * FROM results_unified;
"

# ─── Post-processing ─────────────────────────────────────────────────────────
echo "Aggiunta header e pulizia..."
HEADER="origin|month|delay_band|num_flights|avg_dep|avg_arr|top_cause_1|top_cause_2|top_cause_3"

# shellcheck disable=SC2012
PART_FILE=$(ls "${OUTPUT_DIR}"/000000_* 2>/dev/null | head -n 1)

if [ -n "$PART_FILE" ]; then
    echo "$HEADER" > "${OUTPUT_DIR}/output.csv"
    cat "$PART_FILE" >> "${OUTPUT_DIR}/output.csv"
    rm -f "$PART_FILE"
else
    echo "ERRORE: File risultati non trovato!"
    exit 1
fi

# Pulisce eventuali file temporanei di Hive e file nascosti .crc
rm -f "${OUTPUT_DIR}"/000000_*
rm -f "${OUTPUT_DIR}"/.*.crc

echo "End: $(date)"
echo "Tempo di esecuzione: $(( $(date +%s) - START ))s"

# ─── Anteprima ────────────────────────────────────────────────────────────────
echo ""
echo "=== Prime 10 righe ==="
head -n 10 "${OUTPUT_DIR}/output.csv"