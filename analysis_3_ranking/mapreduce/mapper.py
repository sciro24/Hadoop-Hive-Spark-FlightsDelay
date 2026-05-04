#!/usr/bin/env python3
"""
Mapper Analisi 3.3 — Ranking coppie compagnia-aeroporto
Fase 1: emette due tipi di chiave:
  - ("carrier", origin, carrier) → (dep_delay, arr_delay, cancelled, 1)
  - ("airport", origin, "__ALL__") → (dep_delay, arr_delay, 0, 1)
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    fields = line.split(",")
    if len(fields) < 15:
        continue

    # Salta header
    if fields[0] == "fl_date" or fields[2] == "month":
        continue

    try:
        origin    = fields[4].strip()
        carrier   = fields[3].strip()
        dep_delay = fields[6].strip()
        arr_delay = fields[7].strip()
        cancelled = fields[8].strip()

        if not origin or not carrier:
            continue

        dep = float(dep_delay) if dep_delay not in ("", "nan", "NA") else 0.0
        arr = float(arr_delay) if arr_delay not in ("", "nan", "NA") else 0.0
        canc = float(cancelled) if cancelled not in ("", "nan", "NA") else 0.0

        # Emetti riga per (compagnia, aeroporto)
        print(f"carrier\t{origin}\t{carrier}\t{dep}\t{arr}\t{canc}\t1")

        # Emetti riga per media globale aeroporto
        print(f"airport\t{origin}\t__ALL__\t{dep}\t{arr}\t0\t1")

    except (ValueError, IndexError):
        continue