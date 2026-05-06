#!/usr/bin/env python3
"""
Mapper Analisi 3.3 — Ranking coppie compagnia-aeroporto
Fase 1: emette due tipi di chiave:
  - (origin, "carrier", carrier) → (dep_delay, arr_delay, cancelled, 1, dep_valid, arr_valid)
  - (origin, "airport", "__ALL__") → (dep_delay, arr_delay, 0, 1, dep_valid, arr_valid)

NULL handling: dep_delay e arr_delay NULL vengono emessi come "NULL"
e non contribuiscono alle medie nel reducer (ma contano per num_flights e cancel_rate).
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

        canc = float(cancelled) if cancelled not in ("", "nan", "NA") else 0.0

        # dep_delay e arr_delay: emetti il valore numerico o "NULL"
        dep_str = dep_delay if dep_delay not in ("", "nan", "NA") else "NULL"
        arr_str = arr_delay if arr_delay not in ("", "nan", "NA") else "NULL"

        # Valida i valori numerici prima di emetterli
        if dep_str != "NULL":
            float(dep_str)  # test parsing
        if arr_str != "NULL":
            float(arr_str)  # test parsing

        # origin è la prima colonna → Hadoop Streaming raggruppa per origin
        print(f"{origin}\tcarrier\t{carrier}\t{dep_str}\t{arr_str}\t{canc}\t1")
        print(f"{origin}\tairport\t__ALL__\t{dep_str}\t{arr_str}\t0\t1")

    except (ValueError, IndexError):
        continue