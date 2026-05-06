#!/usr/bin/env bash
# ─── Benchmark completo: tutte le analisi × tutte le tecnologie × tutti i sample ─
set -e

# ─── Silenzia WARN Hadoop/Hive/Spark ─────────────────────────────────────────
export HADOOP_ROOT_LOGGER="ERROR,console"
export HADOOP_OPTS="-Dlog4j.rootLogger=ERROR,console \
                    -Dlog4j.logger.org.apache.hadoop=ERROR \
                    -Dlog4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR \
                    $HADOOP_OPTS"
export HIVE_OPTS="--hiveconf hive.root.logger=ERROR,console $HIVE_OPTS"

# ─── Configurazione ───────────────────────────────────────────────────────────
TRACKER="python3 benchmarks/benchmark_tracker.py"
SAMPLES_DIR="data/samples"
CLEANED="data/cleaned/flight_data_2024_cleaned.csv"

declare -A SAMPLE_FRACS
SAMPLE_FRACS["010pct"]="0.10"
SAMPLE_FRACS["025pct"]="0.25"
SAMPLE_FRACS["050pct"]="0.50"
SAMPLE_FRACS["125pct"]="1.25"
SAMPLE_FRACS["150pct"]="1.50"

declare -A TECHS
TECHS["3.1"]="mapreduce hive spark_sql"
TECHS["3.2"]="hive spark_core spark_sql"
TECHS["3.3"]="mapreduce spark_core spark_sql"

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

ANALYSES=${ANALYSES:-"3.1 3.2 3.3"}

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         BENCHMARK SUITE — Flight Delay 2024             ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Analisi:  $ANALYSES"
echo "║  Sample:   010pct 025pct 050pct full 125pct 150pct"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

mkdir -p logs

# Genera i sample se non esistono
for pct in "010pct" "025pct" "050pct" "125pct" "150pct"; do
    sample_file="${SAMPLES_DIR}/sample_${pct}.csv"
    if [ ! -f "$sample_file" ]; then
        echo "⚠️  Sample ${pct} non trovato, lo genero..."
        python3 data_preparation/generate_samples.py \
            --fractions "${SAMPLE_FRACS[$pct]}"
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

        # ── Ordine: 010 → 025 → 050 → full(100%) → 125 → 150 ────────────────
        for pct in "010pct" "025pct" "050pct"; do
            # Seleziona estensione in base alla tecnologia
            ext="parquet"
            if [ "$tech" == "mapreduce" ]; then ext="csv"; fi
            
            input="${SAMPLES_DIR}/sample_${pct}.${ext}"
            if [ ! -d "$input" ] && [ ! -f "$input" ]; then echo "❌  $input non trovato, skip."; continue; fi
            
            echo ""
            echo "▶  Analisi ${analysis} | ${tech} | sample ${pct} (${ext})"
            echo "   Input: $input"
            export BENCHMARK_INPUT="$input"
            $TRACKER \
                --analysis "$analysis" --tech "$tech" \
                --input    "$input"    --cmd  "$script" \
                --notes    "sample_${pct}" \
            && TOTAL_JOBS=$((TOTAL_JOBS + 1)) \
            || { FAILED_JOBS=$((FAILED_JOBS + 1)); TOTAL_JOBS=$((TOTAL_JOBS + 1)); }
        done

        # ── Full dataset 100% ─────────────────────────────────────────────────
        ext="parquet"
        if [ "$tech" == "mapreduce" ]; then ext="csv"; fi
        
        input_full="${CLEANED%.csv}.${ext}"
        
        echo ""
        echo "▶  Analisi ${analysis} | ${tech} | 100% (${ext})"
        echo "   Input: $input_full"
        export BENCHMARK_INPUT="$input_full"
        $TRACKER \
            --analysis "$analysis" --tech "$tech" \
            --input    "$input_full"  --cmd  "$script" \
            --notes    "full_dataset" \
        && TOTAL_JOBS=$((TOTAL_JOBS + 1)) \
        || { FAILED_JOBS=$((FAILED_JOBS + 1)); TOTAL_JOBS=$((TOTAL_JOBS + 1)); }

        # ── 125% e 150% ───────────────────────────────────────────────────────
        for pct in "125pct" "150pct"; do
            ext="parquet"
            if [ "$tech" == "mapreduce" ]; then ext="csv"; fi
            
            input="${SAMPLES_DIR}/sample_${pct}.${ext}"
            if [ ! -d "$input" ] && [ ! -f "$input" ]; then echo "❌  $input non trovato, skip."; continue; fi
            
            echo ""
            echo "▶  Analisi ${analysis} | ${tech} | sample ${pct} (${ext})"
            echo "   Input: $input"
            export BENCHMARK_INPUT="$input"
            $TRACKER \
                --analysis "$analysis" --tech "$tech" \
                --input    "$input"    --cmd  "$script" \
                --notes    "sample_${pct}" \
            && TOTAL_JOBS=$((TOTAL_JOBS + 1)) \
            || { FAILED_JOBS=$((FAILED_JOBS + 1)); TOTAL_JOBS=$((TOTAL_JOBS + 1)); }
        done

    done
done

# ─── Raccolta prime 10 righe ──────────────────────────────────────────────────
echo ""
echo "Raccolta sample output (prime 10 righe)..."
python3 benchmarks/collect_samples.py

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