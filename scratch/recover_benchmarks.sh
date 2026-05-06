#!/bin/bash
set -e

# 1. Analisi 3.2 — Spark SQL
echo "Recupero Analisi 3.2 Spark SQL..."
for pct in "010pct" "025pct" "050pct" "full" "125pct" "150pct"; do
    if [ "$pct" == "full" ]; then 
        input="data/cleaned/flight_data_2024_cleaned.parquet"
        note="full_dataset"
    else 
        input="data/samples/sample_${pct}.parquet"
        note="sample_${pct}"
    fi
    BENCHMARK_INPUT="$input" .venv/bin/python3 benchmarks/benchmark_tracker.py \
        --analysis 3.2 --tech spark_sql --input "$input" \
        --cmd "./analysis_2_delay_report/spark_sql/run.sh" --notes "$note"
done

# 2. Analisi 3.3 — Spark Core e Spark SQL
echo "Recupero Analisi 3.3 Spark Core e SQL..."
for tech in "spark_core" "spark_sql"; do
    for pct in "010pct" "025pct" "050pct" "full" "125pct" "150pct"; do
        if [ "$pct" == "full" ]; then 
            input="data/cleaned/flight_data_2024_cleaned.parquet"
            note="full_dataset"
        else 
            input="data/samples/sample_${pct}.parquet"
            note="sample_${pct}"
        fi
        BENCHMARK_INPUT="$input" .venv/bin/python3 benchmarks/benchmark_tracker.py \
            --analysis 3.3 --tech "$tech" --input "$input" \
            --cmd "./analysis_3_ranking/${tech}/run.sh" --notes "$note"
    done
done

# 3. Aggiorna i top 10 samples
echo "Aggiornamento campioni finali..."
python3 benchmarks/collect_samples.py
