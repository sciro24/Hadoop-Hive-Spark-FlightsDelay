#!/usr/bin/env python3
"""
Tracker centralizzato dei benchmark.
Wrappa qualsiasi comando (MapReduce, Hive, Spark) misurando:
  - tempo di esecuzione
  - dimensione input
  - tecnologia / analisi
e appende il risultato a benchmarks/results_local.csv

Uso:
  python3 benchmarks/benchmark_tracker.py \
      --analysis  "3.1" \
      --tech      "hive" \
      --input     "data/cleaned/flight_data_2024_cleaned.csv" \
      --cmd       "./analysis_1_airline_stats/hive/run.sh"
"""
import argparse
import csv
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

RESULTS_FILE = Path("benchmarks/results_local.csv")
FIELDNAMES   = [
    "timestamp", "analysis", "technology", "environment",
    "input_file", "input_size_mb", "input_rows",
    "elapsed_sec", "exit_code", "notes"
]

def get_file_info(filepath: str):
    p = Path(filepath)
    if not p.exists():
        return 0.0, "N/A"
    size_mb = round(p.stat().st_size / (1024 * 1024), 2)
    # Conta righe velocemente
    with open(p, "rb") as f:
        rows = sum(1 for _ in f) - 1  # -1 per header
    return size_mb, rows

def append_result(row: dict):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_header = not RESULTS_FILE.exists()
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

def run(analysis, tech, input_file, cmd, env="local", notes=""):
    print(f"\n{'='*60}")
    print(f"  Analisi:    {analysis}")
    print(f"  Tecnologia: {tech}")
    print(f"  Ambiente:   {env}")
    print(f"  Comando:    {cmd}")
    print(f"{'='*60}\n")

    size_mb, n_rows = get_file_info(input_file)
    print(f"  Input: {size_mb} MB | {n_rows:,} righe\n")

    start = time.time()
    result = subprocess.run(cmd, shell=True)
    elapsed = round(time.time() - start, 2)

    row = {
        "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "analysis":     analysis,
        "technology":   tech,
        "environment":  env,
        "input_file":   input_file,
        "input_size_mb": size_mb,
        "input_rows":   n_rows,
        "elapsed_sec":  elapsed,
        "exit_code":    result.returncode,
        "notes":        notes
    }

    append_result(row)

    status = "✅ OK" if result.returncode == 0 else "❌ FAILED"
    print(f"\n{status} — Tempo: {elapsed}s")
    print(f"Risultato salvato in: {RESULTS_FILE}\n")

    return result.returncode

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark tracker per job Big Data")
    parser.add_argument("--analysis", required=True, help="Es: 3.1, 3.2, 3.3")
    parser.add_argument("--tech",     required=True, help="mapreduce | hive | spark_sql | spark_core")
    parser.add_argument("--input",    required=True, help="Path al file CSV di input")
    parser.add_argument("--cmd",      required=True, help="Comando da eseguire")
    parser.add_argument("--env",      default="local", help="local | cluster")
    parser.add_argument("--notes",    default="",    help="Note opzionali (es. '25pct dataset')")
    args = parser.parse_args()

    exit(run(args.analysis, args.tech, args.input, args.cmd, args.env, args.notes))