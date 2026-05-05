#!/usr/bin/env python3
"""
Genera sample crescenti del dataset pulito per l'analisi sperimentale.

Sample prodotti di default:
  - 10%  → ~703k  righe
  - 25%  → ~1.76M righe
  - 50%  → ~3.52M righe
  - 125% → ~8.80M righe  (100% + 25% repliche casuali)
  - 150% → ~10.6M righe  (100% + 50% repliche casuali)

Uso:
  python3 data_preparation/generate_samples.py
  python3 data_preparation/generate_samples.py --fractions 0.10 0.25 0.50 1.25 1.50
  python3 data_preparation/generate_samples.py --input data/cleaned/flight_data_2024_cleaned.csv
"""
import argparse
import time
from pathlib import Path

import pandas as pd

SEED = 42


def human_size(path: Path) -> str:
    mb = path.stat().st_size / (1024 * 1024)
    return f"{mb:.1f} MB"


def generate(input_path: Path, output_dir: Path, fractions: list[float]):
    print(f"\n{'='*60}")
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_dir}")
    print(f"  Frazioni: {[f'{int(f*100)}%' for f in fractions]}")
    print(f"{'='*60}\n")

    output_dir.mkdir(parents=True, exist_ok=True)

    # ─── Caricamento ──────────────────────────────────────────────────────────
    t0 = time.time()
    print("Caricamento dataset...", end=" ", flush=True)
    df = pd.read_csv(input_path, low_memory=False)
    total_rows = len(df)
    print(f"✅  {total_rows:,} righe | {human_size(input_path)} ({time.time()-t0:.1f}s)\n")

    generated = []

    # ─── Sample frazionali ────────────────────────────────────────────────────
    for frac in fractions:
        pct  = int(frac * 100)
        name = f"sample_{pct:03d}pct.csv"
        out  = output_dir / name

        t1 = time.time()

        if frac <= 1.0:
            # Campionamento normale senza rimpiazzo
            sample = df.sample(frac=frac, random_state=SEED).reset_index(drop=True)
        else:
            # Frazioni > 100%: dataset completo + repliche casuali della parte eccedente
            extra_frac = frac - 1.0
            extra_n    = int(total_rows * extra_frac)
            extra      = df.sample(n=extra_n, random_state=SEED, replace=True).reset_index(drop=True)
            sample     = pd.concat([df, extra], ignore_index=True)

        sample.to_csv(out, index=False)

        rows = len(sample)
        print(f"  [{pct:>3}%]  {rows:>9,} righe  →  {name}  ({human_size(out)})  [{time.time()-t1:.1f}s]")
        generated.append((name, rows, human_size(out)))

    # ─── Riepilogo ────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  {'File':<30} {'Righe':>12}  {'Dimensione':>10}")
    print(f"{'─'*60}")
    for name, rows, size in generated:
        print(f"  {name:<30} {rows:>12,}  {size:>10}")
    print(f"{'─'*60}")
    print(f"\n✅  {len(generated)} sample generati in: {output_dir}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera sample crescenti del dataset")
    parser.add_argument(
        "--input",
        default="data/cleaned/flight_data_2024_cleaned.csv",
        help="Path al CSV pulito (default: data/cleaned/flight_data_2024_cleaned.csv)"
    )
    parser.add_argument(
        "--output-dir",
        default="data/samples",
        help="Cartella di output (default: data/samples)"
    )
    parser.add_argument(
        "--fractions", nargs="+", type=float,
        default=[0.10, 0.25, 0.50, 1.25, 1.50],
        help="Frazioni da generare (default: 0.10 0.25 0.50 1.25 1.50)"
    )
    args = parser.parse_args()

    generate(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        fractions=args.fractions,
    )