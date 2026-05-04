#!/bin/bash
# ─── Analisi 3.2 — Hive ──────────────────────────────────────────────────────
set -e

OUTPUT_DIR="results/analysis_2/hive"
HQL="analysis_2_delay_report/hive/queries.hql"

echo "=== Analisi 3.2 — Hive ==="
echo "Start: $(date)"
START=$(date +%s)

mkdir -p "$OUTPUT_DIR"

# Esegui HQL (riusa la tabella flights_clean già caricata dalla 3.1)
hive -f "$HQL" 2>&1 | tee "$OUTPUT_DIR/hive_log.txt"

# Esporta risultati
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