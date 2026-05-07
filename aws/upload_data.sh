#!/bin/bash
# ─── Upload dati e script su S3 ──────────────────────────────────────────────
# Esegui questo script UNA VOLTA dal tuo Mac prima di creare il cluster.
# Prerequisito: AWS CLI configurato (aws configure) con le credenziali Academy.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Carica la configurazione
source "$SCRIPT_DIR/config.sh"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              UPLOAD DATI SU S3                          ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Bucket: s3://${S3_BUCKET}/${S3_PREFIX}"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

cd "$PROJECT_ROOT"

# ── Crea bucket se non esiste ─────────────────────────────────────────────────
echo "► Verifica/creazione bucket S3..."
aws s3 mb "s3://${S3_BUCKET}" --region "$EMR_REGION" 2>/dev/null || echo "  (bucket già esistente)"

# ── Upload dataset cleaned ────────────────────────────────────────────────────
echo "► Upload dataset cleaned..."
aws s3 cp data/cleaned/flight_data_2024_cleaned.csv \
    "${S3_DATA}/cleaned/flight_data_2024_cleaned.csv" \
    --no-progress

# Upload parquet (directory)
aws s3 sync data/cleaned/flight_data_2024_cleaned.parquet/ \
    "${S3_DATA}/cleaned/flight_data_2024_cleaned.parquet/" \
    --no-progress

# ── Upload samples ────────────────────────────────────────────────────────────
echo "► Upload samples CSV e Parquet..."
for pct in "010pct" "025pct" "050pct" "125pct" "150pct"; do
    # CSV
    if [ -f "data/samples/sample_${pct}.csv" ]; then
        aws s3 cp "data/samples/sample_${pct}.csv" \
            "${S3_DATA}/samples/sample_${pct}.csv" --no-progress
    fi
    # Parquet (directory)
    if [ -d "data/samples/sample_${pct}.parquet" ]; then
        aws s3 sync "data/samples/sample_${pct}.parquet/" \
            "${S3_DATA}/samples/sample_${pct}.parquet/" --no-progress
    fi
done

# ── Upload codice Python/script ───────────────────────────────────────────────
echo "► Upload codice sorgente..."
aws s3 sync . "${S3_SCRIPTS}/" \
    --no-progress \
    --exclude "*.DS_Store" \
    --exclude "data/*" \
    --exclude "results/*" \
    --exclude "eda/plots/*" \
    --exclude "benchmarks/plots/*" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*"

# ── Upload bootstrap ──────────────────────────────────────────────────────────
echo "► Upload bootstrap script..."
aws s3 cp aws/bootstrap.sh "${S3_SCRIPTS}/bootstrap.sh" --no-progress

echo ""
echo "✅  Upload completato."
echo ""
echo "Path S3 pronti:"
echo "  Data:    ${S3_DATA}/"
echo "  Scripts: ${S3_SCRIPTS}/"
echo "  Logs:    ${S3_LOGS}/"
