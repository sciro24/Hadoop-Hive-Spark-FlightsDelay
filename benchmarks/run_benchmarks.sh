#!/usr/bin/env bash
# ─── Benchmark completo: tutte le analisi × tutte le tecnologie × tutti i sample ─
# Salva i risultati in benchmarks/results_local.csv tramite benchmark_tracker.py
#
# Uso:
#   chmod +x benchmarks/run_benchmarks.sh
#   ./benchmarks/run_benchmarks.sh
#
# Per eseguire solo una analisi:
#   ANALYSES="3.1" ./benchmarks/run_benchmarks.sh
#
# Per eseguire solo alcuni sample:
#   SAMPLES="010pct 100pct" ./benchmarks/run_benchmarks.sh

set -e

# ─── Configurazione ───────────────────────────────────────────────────────────
TRACKER="python3 benchmarks/benchmark_tracker.py"
SAMPLES_DIR="data/samples"
CLEANED="data/cleaned/flight_data_2024_cleaned.csv"

# Sample da testare (modifica qui per aggiungerne/rimuoverne)
SAMPLES_PCT=("010pct" "025pct" "050pct")

# Analisi e relative tecnologie
declare -A TECHS
TECHS["3.1"]="mapreduce hive spark_sql"
TECHS["3.2"]="hive spark_core spark_sql"
TECHS["3.3"]="mapreduce spark_core spark_sql"

# Script da eseguire per ogni (analisi, tecnologia)
declare -A SCRIPTS
SCRIPTS["3.1:mapreduce"]="./analysis_1_airline_stats/mapreduce/run.sh"
SCRIPTS["3.1:hive"]="./analysis_1_airline_stats/hive/run.sh"
SCRIPTS["3.1:spark_sql"]="./analysis_1_airline_stats/spark_sql/run.sh"
SCRIPTS["3.2:hive"]="./analysis_2_delay_report/hive/run.sh"
SCRIPTS["3.2:spark_core"]="./analysis_2_delay_report/spark_core/run.sh"
SCRIPTS["3.2:spark_sql"]="./analysis_2_delay_report/spark_sql/run.sh"
SCRIPTS["3.3:mapreduce"]="./analysis_3_ranking/mapreduce/run.sh"
SCRIPTS["3.3:spark_core"]="./analysis_3_ranking/spark_core/run.sh"
SCRIPTS["3.3:spark_sql"]="./analysis_3_ranking/spark_sql/run.sh"

# Analisi da eseguire (override con env var ANALYSES)
ANALYSES=${ANALYSES:-"3.1 3.2 3.3"}

# ─── Verifica prerequisiti ────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         BENCHMARK SUITE — Flight Delay 2024             ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Analisi:  $ANALYSES"
echo "║  Sample:   ${SAMPLES_PCT[*]} + cleaned"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Genera i sample se non esistono
for pct in "${SAMPLES_PCT[@]}"; do
    sample_file="${SAMPLES_DIR}/sample_${pct}.csv"
    if [ ! -f "$sample_file" ]; then
        echo "⚠️  Sample ${pct} non trovato, lo genero..."
        python3 data_preparation/generate_samples.py \
            --fractions $(echo $pct | sed 's/pct//' | awk '{printf "%.2f", $1/100}')
    fi
done

TOTAL_JOBS=0
FAILED_JOBS=0
START_ALL=$(date +%s)

# ─── Loop principale ──────────────────────────────────────────────────────────
for analysis in $ANALYSES; do
    techs="${TECHS[$analysis]}"
    if [ -z "$techs" ]; then
        echo "⚠️  Analisi $analysis non configurata, skip."
        continue
    fi

    for tech in $techs; do
        script="${SCRIPTS[$analysis:$tech]}"
        if [ -z "$script" ]; then
            echo "⚠️  Script per $analysis:$tech non trovato, skip."
            continue
        fi

        # ── Esegui su ogni sample ─────────────────────────────────────────────
        for pct in "${SAMPLES_PCT[@]}"; do
            input="${SAMPLES_DIR}/sample_${pct}.csv"

            if [ ! -f "$input" ]; then
                echo "❌  File $input non trovato, skip."
                continue
            fi

            echo ""
            echo "▶  Analisi ${analysis} | ${tech} | sample ${pct}"
            echo "   Input: $input"

            # Sovrascrive INPUT_PATH per i job Spark/Hive che lo leggono
            # tramite variabile d'ambiente (aggiunta nei run.sh)
            export BENCHMARK_INPUT="$input"

            $TRACKER \
                --analysis "$analysis" \
                --tech     "$tech" \
                --input    "$input" \
                --cmd      "$script" \
                --notes    "sample_${pct}" \
            && TOTAL_JOBS=$((TOTAL_JOBS + 1)) \
            || { FAILED_JOBS=$((FAILED_JOBS + 1)); TOTAL_JOBS=$((TOTAL_JOBS + 1)); }
        done

        # ── Esegui sul dataset completo cleaned ───────────────────────────────
        echo ""
        echo "▶  Analisi ${analysis} | ${tech} | 100% (cleaned)"
        echo "   Input: $CLEANED"

        export BENCHMARK_INPUT="$CLEANED"

        $TRACKER \
            --analysis "$analysis" \
            --tech     "$tech" \
            --input    "$CLEANED" \
            --cmd      "$script" \
            --notes    "full_dataset" \
        && TOTAL_JOBS=$((TOTAL_JOBS + 1)) \
        || { FAILED_JOBS=$((FAILED_JOBS + 1)); TOTAL_JOBS=$((TOTAL_JOBS + 1)); }

    done
done

# ─── Riepilogo finale ─────────────────────────────────────────────────────────
END_ALL=$(date +%s)
ELAPSED_ALL=$((END_ALL - START_ALL))

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                  BENCHMARK COMPLETATI                   ║"
echo "╠══════════════════════════════════════════════════════════╣"
printf  "║  Job totali:   %-40s ║\n" "$TOTAL_JOBS"
printf  "║  Successi:     %-40s ║\n" "$((TOTAL_JOBS - FAILED_JOBS))"
printf  "║  Falliti:      %-40s ║\n" "$FAILED_JOBS"
printf  "║  Tempo totale: %-40s ║\n" "${ELAPSED_ALL}s (~$((ELAPSED_ALL/60))min)"
printf  "║  Risultati:    %-40s ║\n" "benchmarks/results_local.csv"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""