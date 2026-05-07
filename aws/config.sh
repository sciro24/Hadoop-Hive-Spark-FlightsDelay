#!/bin/bash
# ─── Configurazione AWS EMR ───────────────────────────────────────────────────
# MODIFICA QUESTI VALORI prima di eseguire qualsiasi script AWS

# ── S3 ────────────────────────────────────────────────────────────────────────
export S3_BUCKET="flights-delay-diego-2024"        # ← CAMBIA QUI
export S3_PREFIX="flights-delay"

# Path S3 derivati (non modificare)
export S3_DATA="s3://${S3_BUCKET}/${S3_PREFIX}/data"
export S3_RESULTS="s3://${S3_BUCKET}/${S3_PREFIX}/results"
export S3_LOGS="s3://${S3_BUCKET}/${S3_PREFIX}/logs"
export S3_SCRIPTS="s3://${S3_BUCKET}/${S3_PREFIX}/scripts"
export S3_OUTPUT_BASE="$S3_RESULTS"

# ── Cluster ───────────────────────────────────────────────────────────────────
export EMR_CLUSTER_NAME="FlightsDelay-BigData"
export EMR_REGION="us-east-1"                  # ← Regione AWS Academy
export EMR_RELEASE="emr-7.2.0"                 # Spark 3.5, Hadoop 3.4, Hive 3.1
export MASTER_TYPE="m5.xlarge"
export WORKER_TYPE="m5.xlarge"
export NUM_WORKERS=3                            # 1 master + 3 worker

# ── Spark Executor ────────────────────────────────────────────────────────────
export EXECUTOR_MEMORY="4g"
export EXECUTOR_CORES="2"
export NUM_EXECUTORS="6"                        # ~2 per worker
export SHUFFLE_PARTITIONS="100"
export NUM_REDUCERS="6"                         # per MapReduce

# ── Cluster mode flag (attivato da run_benchmarks_cluster.sh) ─────────────────
export CLUSTER_MODE="true"
