#!/usr/bin/env python3
"""
Tracker centralizzato dei benchmark.
Wrappa qualsiasi comando (MapReduce, Hive, Spark) misurando:
  - tempo di esecuzione
  - dimensione input
  - tecnologia / analisi
e salva/sovrascrive il risultato in benchmarks/results_{env}.csv

Uso:
  python3 benchmarks/benchmark_tracker.py \
      --analysis  "3.1" \
      --tech      "hive" \
      --input     "data/cleaned/flight_data_2024_cleaned.csv" \
      --cmd       "./analysis_1_airline_stats/hive/run.sh" \
      [--env      "local|cluster"]
"""
import argparse
import csv
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

FIELDNAMES = [
    "timestamp", "analysis", "technology", "environment",
    "input_file", "input_size_mb", "input_rows",
    "elapsed_sec", "exit_code", "notes"
]


def results_file(env: str) -> Path:
    name = "results_cluster.csv" if env == "cluster" else "results_local.csv"
    return Path("benchmarks") / name


def _s3_file_info(s3_path: str):
    """Ottieni dimensione da S3 via aws cli. Restituisce (size_mb, 'N/A')."""
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", "--recursive", s3_path],
            capture_output=True, text=True, timeout=30
        )
        total_bytes = 0
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                try:
                    total_bytes += int(parts[2])
                except ValueError:
                    pass
        size_mb = round(total_bytes / (1024 * 1024), 2) if total_bytes else 0.0
        return size_mb, "N/A"
    except Exception:
        return 0.0, "N/A"


try:
    import pyarrow.parquet as pq
    _has_pyarrow = True
except ImportError:
    _has_pyarrow = False


def get_file_info(filepath: str):
    if filepath.startswith("s3://"):
        return _s3_file_info(filepath)

    p = Path(filepath)
    if not p.exists():
        return 0.0, 0

    if p.is_dir():
        size_bytes = sum(f.stat().st_size for f in p.rglob('*') if f.is_file())
        size_mb = round(size_bytes / (1024 * 1024), 2)
        if _has_pyarrow:
            try:
                rows = sum(pq.read_metadata(f).num_rows for f in p.rglob('*.parquet') if f.is_file())
            except Exception:
                rows = "N/A"
        else:
            rows = "N/A"
    else:
        size_mb = round(p.stat().st_size / (1024 * 1024), 2)
        try:
            with open(p, "rb") as f:
                rows = sum(1 for _ in f) - 1
        except Exception:
            rows = "N/A"

    return size_mb, rows


def load_results(env: str) -> list[dict]:
    rf = results_file(env)
    if not rf.exists():
        return []
    with open(rf, newline="") as f:
        return list(csv.DictReader(f))


def save_results(rows: list[dict], env: str):
    rf = results_file(env)
    rf.parent.mkdir(parents=True, exist_ok=True)
    with open(rf, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def upsert_result(new_row: dict, env: str):
    rows = load_results(env)
    key = ("analysis", "technology", "environment", "input_file")
    replaced = False
    for i, row in enumerate(rows):
        if all(row.get(k) == new_row[k] for k in key):
            rows[i] = new_row
            replaced = True
            break
    if not replaced:
        rows.append(new_row)
    save_results(rows, env)
    return replaced


def run(analysis, tech, input_file, cmd, env="local", notes=""):
    print(f"\n{'='*60}")
    print(f"  Analisi:    {analysis}")
    print(f"  Tecnologia: {tech}")
    print(f"  Ambiente:   {env}")
    print(f"  Comando:    {cmd}")
    print(f"{'='*60}\n")

    size_mb, n_rows = get_file_info(input_file)
    rows_display = f"{n_rows:,}" if isinstance(n_rows, int) else n_rows
    print(f"  Input: {size_mb} MB | {rows_display} righe\n")

    start = time.time()
    result = subprocess.run(cmd, shell=True)
    elapsed = round(time.time() - start, 2)

    row = {
        "timestamp":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "analysis":      analysis,
        "technology":    tech,
        "environment":   env,
        "input_file":    input_file,
        "input_size_mb": size_mb,
        "input_rows":    n_rows,
        "elapsed_sec":   elapsed,
        "exit_code":     result.returncode,
        "notes":         notes
    }

    replaced = upsert_result(row, env)
    action   = "aggiornato" if replaced else "aggiunto"

    status = "✅ OK" if result.returncode == 0 else "❌ FAILED"
    print(f"\n{status} — Tempo: {elapsed}s")
    print(f"Record {action} in: {results_file(env)}\n")

    return result.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark tracker per job Big Data")
    parser.add_argument("--analysis", required=True, help="Es: 3.1, 3.2, 3.3")
    parser.add_argument("--tech",     required=True, help="mapreduce | hive | spark_sql | spark_core")
    parser.add_argument("--input",    required=True, help="Path al file di input (locale o s3://)")
    parser.add_argument("--cmd",      required=True, help="Comando da eseguire")
    parser.add_argument("--env",      default="local", help="local | cluster")
    parser.add_argument("--notes",    default="",    help="Note opzionali (es. '25pct dataset')")
    args = parser.parse_args()

    exit(run(args.analysis, args.tech, args.input, args.cmd, args.env, args.notes))
