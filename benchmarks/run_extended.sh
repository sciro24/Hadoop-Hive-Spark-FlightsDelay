#!/bin/bash
# Esegue il benchmark solo sui sample 125pct e 150pct
set -e

SAMPLES=("data/samples/sample_125pct.csv" "data/samples/sample_150pct.csv")
RESULTS_CSV="benchmarks/results_local.csv"

# Crea header se il file non esiste
if [ ! -f "$RESULTS_CSV" ]; then
    echo "timestamp,analysis,technology,environment,input_file,input_size_mb,input_rows,elapsed_sec,exit_code,notes" \
        > "$RESULTS_CSV"
fi

run_job() {
    local analysis="$1"
    local tech="$2"
    local script="$3"
    local input="$4"

    local label=$(basename "$input" .csv | sed 's/sample_//')
    local size_mb=$(du -m "$input" | cut -f1)
    local rows=$(tail -n +2 "$input" | wc -l | tr -d ' ')
    local ts=$(date '+%Y-%m-%d %H:%M:%S')

    echo "--- $analysis $tech | $label ---"
    local start=$(date +%s)

    BENCHMARK_INPUT="$input" /opt/homebrew/bin/bash "$script"
    local exit_code=$?

    local end=$(date +%s)
    local elapsed=$((end - start))

    echo "$ts,$analysis,$tech,local,$input,$size_mb,$rows,$elapsed,$exit_code,$label" \
        >> "$RESULTS_CSV"
}

for INPUT in "${SAMPLES[@]}"; do
    run_job "3.1" "mapreduce"  "analysis_1_airline_stats/mapreduce/run.sh"   "$INPUT"
    run_job "3.1" "hive"       "analysis_1_airline_stats/hive/run.sh"        "$INPUT"
    run_job "3.1" "spark_sql"  "analysis_1_airline_stats/spark_sql/run.sh"   "$INPUT"
    run_job "3.2" "hive"       "analysis_2_delay_report/hive/run.sh"         "$INPUT"
    run_job "3.2" "spark_core" "analysis_2_delay_report/spark_core/run.sh"   "$INPUT"
    run_job "3.2" "spark_sql"  "analysis_2_delay_report/spark_sql/run.sh"    "$INPUT"
    run_job "3.3" "mapreduce"  "analysis_3_ranking/mapreduce/run.sh"         "$INPUT"
    run_job "3.3" "spark_core" "analysis_3_ranking/spark_core/run.sh"        "$INPUT"
    run_job "3.3" "spark_sql"  "analysis_3_ranking/spark_sql/run.sh"         "$INPUT"
done

echo ""
echo "=== Nuovi risultati aggiunti in $RESULTS_CSV ==="
grep -E "125pct|150pct" "$RESULTS_CSV"