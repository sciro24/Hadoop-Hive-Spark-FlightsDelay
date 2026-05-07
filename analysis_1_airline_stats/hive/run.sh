#!/bin/bash
# ─── Analisi 3.1 — Hive ──────────────────────────────────────────────────────
set -e
set -o pipefail

INPUT="${BENCHMARK_INPUT:-data/cleaned/flight_data_2024_cleaned.csv}"
INPUT_FILENAME=$(basename "$INPUT")

HQL="analysis_1_airline_stats/hive/queries.hql"
OUTPUT_DIR="$(pwd)/results/analysis_1/hive"

echo "=== Analisi 3.1 — Hive ==="
echo "Input: $INPUT"
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

if [ "${CLUSTER_MODE:-false}" = "true" ]; then
    # ── Modalità cluster AWS EMR ──────────────────────────────────────────────
    # I dati sono già in S3; DATA_LOCATION è la path S3 passata dal benchmark runner
    DATA_LOCATION="${DATA_LOCATION:-$INPUT}"
    S3_BUCKET="${S3_BUCKET:?Imposta la variabile S3_BUCKET}"
    S3_PREFIX="${S3_PREFIX:-flights-delay}"
    S3_RESULTS="s3://${S3_BUCKET}/${S3_PREFIX}/results/analysis_1/hive"

    echo "DATA_LOCATION: $DATA_LOCATION"

    # Crea il database se non esiste
    hive -e "CREATE DATABASE IF NOT EXISTS flights;"

    # Esegui HQL passando la location come variabile
    hive --hivevar DATA_LOCATION="$DATA_LOCATION" -f "$HQL" 2>&1 \
        | grep -v -E "^SLF4J:|^WARNING:|^WARN |^INFO |^Logging initialized|^\s+at " || true

    # Esporta risultati su S3
    hive -e "
USE flights;
INSERT OVERWRITE DIRECTORY '${S3_RESULTS}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT carrier, origin, month, num_flights,
       min_arr_delay, max_arr_delay, avg_arr_delay,
       cancel_rate, months_active
FROM results_airline_stats
ORDER BY carrier, origin, month;
"
    echo "Risultati esportati in: $S3_RESULTS"

else
    # ── Modalità locale ───────────────────────────────────────────────────────
    HDFS_DIR="/user/hive/warehouse/flights_clean"
    RAW_DIR="${OUTPUT_DIR}/raw"

    echo "Caricamento Parquet su HDFS..."
    hadoop fs -rm -r "$HDFS_DIR" 2>/dev/null || true
    hadoop fs -put -f "$INPUT" "$HDFS_DIR" 2>/dev/null

    echo "Esecuzione query Hive..."
    hive --hivevar DATA_LOCATION="$HDFS_DIR" -f "$HQL" 2>&1 \
        | grep -v -E "^SLF4J:|^WARNING:|^Logging initialized|^\s+at |AlreadyExistsException|\
^WARN |^INFO |^Number of reduce|^In order to|^set mapreduce|\
^Starting Job|^Kill Command|^Hadoop job information|Stage-[0-9]+ map =|\
^Launched|^Ended Job|^Stage-Stage|^Total MapReduce|^MapReduce Jobs|\
^MapredLocal|Execution completed|task succeeded|^Moving data|\
Dump the side|Uploaded [0-9]|End of local task|^Stage-[0-9]+ is" \
        | tee "$OUTPUT_DIR/hive_log.txt"

    echo "Esportazione risultati..."
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

    if [[ "$INPUT" == *"cleaned"* ]]; then
        echo "Dataset completo rilevato. Aggiornamento risultati finali..."
        rm -f "$OUTPUT_DIR/output.csv"
        echo "carrier|origin|month|num_flights|min_arr_delay|max_arr_delay|avg_arr_delay|cancel_rate|months_active" \
            > "$OUTPUT_DIR/output.csv"
        cat "$RAW_DIR"/000000_0 >> "$OUTPUT_DIR/output.csv" 2>/dev/null || \
        cat "$RAW_DIR"/* >> "$OUTPUT_DIR/output.csv"
    else
        echo "Dataset sample rilevato ($INPUT_FILENAME). Salto aggiornamento output.csv."
    fi

    rm -rf "$RAW_DIR" "$OUTPUT_DIR/hive_log.txt"
    rm -f "$OUTPUT_DIR/"*.crc
fi

END=$(date +%s)
echo "End: $(date)"
echo "Tempo di esecuzione: $((END - START))s"

echo ""
echo "=== Prime 10 righe ==="
if [ "${CLUSTER_MODE:-false}" != "true" ] && [ -f "$OUTPUT_DIR/output.csv" ]; then
    head -n 10 "$OUTPUT_DIR/output.csv"
fi
