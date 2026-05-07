#!/bin/bash
# ─── Analisi 3.2 — Hive ──────────────────────────────────────────────────────
set -e
set -o pipefail

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.parquet}"
INPUT_FILENAME=$(basename "$INPUT")

HQL="analysis_2_delay_report/hive/queries.hql"
OUTPUT_DIR="$(pwd)/results/analysis_2/hive"

echo "=== Analisi 3.2 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# Definizione comune della tabella Hive (usata sia in locale che in cluster)
create_flights_table() {
    local location="$1"
    hive -e "
CREATE DATABASE IF NOT EXISTS flights;
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
LOCATION '${location}';
"
}

if [ "${CLUSTER_MODE:-false}" = "true" ]; then
    # ── Modalità cluster AWS EMR ──────────────────────────────────────────────
    DATA_LOCATION="${DATA_LOCATION:-$INPUT}"
    S3_BUCKET="${S3_BUCKET:?Imposta la variabile S3_BUCKET}"
    S3_PREFIX="${S3_PREFIX:-flights-delay}"
    S3_RESULTS="s3://${S3_BUCKET}/${S3_PREFIX}/results/analysis_2/hive"

    echo "DATA_LOCATION: $DATA_LOCATION"

    create_flights_table "$DATA_LOCATION"

    echo "Esecuzione query Hive..."
    hive -f "$HQL" 2>&1 \
        | grep -v -E "^SLF4J:|^WARNING:|^WARN |^INFO |^Logging initialized|^\s+at " || true

    # Esporta risultati su S3
    hive -e "
USE flights;
INSERT OVERWRITE DIRECTORY '${S3_RESULTS}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT origin, month, delay_band, num_flights, avg_dep, avg_arr,
       top_cause_1, top_cause_2, top_cause_3
FROM results_unified
ORDER BY origin, month, delay_band;
"
    echo "Risultati esportati in: $S3_RESULTS"

else
    # ── Modalità locale ───────────────────────────────────────────────────────
    HDFS_DIR="/user/hive/warehouse/flights_clean"
    RAW_DIR="${OUTPUT_DIR}/raw"

    echo "Caricamento Parquet su HDFS..."
    hadoop fs -rm -r "$HDFS_DIR" 2>/dev/null || true
    hadoop fs -put -f "$INPUT" "$HDFS_DIR" 2>/dev/null

    create_flights_table "$HDFS_DIR"

    echo "Esecuzione query Hive..."
    hive -f "$HQL" 2>&1 \
        | grep -v -E "^SLF4J:|^WARNING:|^Logging initialized|^\s+at |AlreadyExistsException|\
^WARN |^INFO |^Number of reduce|^In order to|^set mapreduce|\
^Starting Job|^Kill Command|^Hadoop job information|Stage-[0-9]+ map =|\
^Launched|^Ended Job|^Stage-Stage|^Total MapReduce|^MapReduce Jobs|\
^MapredLocal|Execution completed|task succeeded|^Moving data|\
Dump the side|Uploaded [0-9]|End of local task|^Stage-[0-9]+ is" \
        | tee "$OUTPUT_DIR/hive_log.txt"

    rm -f  "$OUTPUT_DIR/output_delay_report.csv"
    rm -f  "$OUTPUT_DIR/output_delay_causes.csv"
    rm -rf "$OUTPUT_DIR/delay_report" "$OUTPUT_DIR/delay_causes"

    echo "Esportazione risultati..."
    hive -e "
USE flights;
INSERT OVERWRITE LOCAL DIRECTORY 'file://${RAW_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
SELECT
    origin, month, delay_band, num_flights, avg_dep, avg_arr,
    top_cause_1, top_cause_2, top_cause_3
FROM results_unified
ORDER BY origin, month, delay_band;
"

    if [[ "$INPUT" == *"cleaned"* ]]; then
        echo "Dataset completo rilevato. Aggiornamento risultati finali..."
        rm -f "${OUTPUT_DIR}/output.csv"
        HEADER="origin|month|delay_band|num_flights|avg_dep|avg_arr|top_cause_1|top_cause_2|top_cause_3"
        PART_FILE=$(ls "${RAW_DIR}"/000000_* 2>/dev/null | head -n 1)
        if [ -n "$PART_FILE" ]; then
            echo "$HEADER" > "${OUTPUT_DIR}/output.csv"
            cat "$PART_FILE" >> "${OUTPUT_DIR}/output.csv"
        fi
    else
        echo "Dataset sample rilevato ($INPUT_FILENAME). Salto aggiornamento output.csv."
    fi

    rm -rf "${RAW_DIR}"
    rm -f "${OUTPUT_DIR}"/.*.crc
fi

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
if [ "${CLUSTER_MODE:-false}" != "true" ] && [ -f "${OUTPUT_DIR}/output.csv" ]; then
    head -n 10 "${OUTPUT_DIR}/output.csv"
fi
