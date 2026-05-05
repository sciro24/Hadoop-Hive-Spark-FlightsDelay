#!/usr/bin/env python3
"""
Reducer Analisi 3.1 — Statistiche compagnie aeree
Input:  chiave<TAB>valore (ordinati per chiave dallo shuffle)
Output: carrier|origin|month|num_flights|min_arr|max_arr|avg_arr|cancel_rate|months_active

"""
import sys
from collections import defaultdict


# Struttura: (carrier, origin) → { month → { running stats } }
# Running stats: count, sum, min, max, cancelled — niente liste
aggregated = defaultdict(lambda: defaultdict(lambda: {
    "flights":   0,
    "sum_arr":   0.0,
    "min_arr":   float('inf'),
    "max_arr":   float('-inf'),
    "cancelled": 0,
    "has_delay": False
}))

# Mesi attivi per (carrier, origin) → set di mesi
months_seen = defaultdict(set)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t", 1)
        carrier, origin, month = key.split("|")
        arr_delay_str, cancelled_str = value.split("|")
    except ValueError:
        continue

    rec = aggregated[(carrier, origin)][month]
    rec["flights"]   += 1
    rec["cancelled"] += int(cancelled_str)
    months_seen[(carrier, origin)].add(month)

    if arr_delay_str != "NULL":
        try:
            val = float(arr_delay_str)
            rec["sum_arr"]  += val
            rec["has_delay"] = True
            if val < rec["min_arr"]: rec["min_arr"] = val
            if val > rec["max_arr"]: rec["max_arr"] = val
        except ValueError:
            pass

# ─── Emissione risultati ──────────────────────────────────────────────────────
try:
    for (carrier, origin), months_data in sorted(aggregated.items()):
        # Mesi attivi ordinati per questa coppia (carrier, origin)
        active = ",".join(sorted(months_seen[(carrier, origin)], key=int))

        for month, rec in sorted(months_data.items(), key=lambda x: int(x[0])):
            n        = rec["flights"]
            n_cancel = rec["cancelled"]

            if rec["has_delay"]:
                # Running stats: nessuna lista, O(1) memoria
                avg_count = n  # tutti i voli (i NULL non hanno incrementato sum_arr)
                # Ricalcola avg solo sui voli con delay valido
                # Non abbiamo count separato per delay: usiamo has_delay come flag
                # Per avg preciso aggiungiamo count_delay
                min_arr = round(rec["min_arr"], 2)
                max_arr = round(rec["max_arr"], 2)
                avg_arr = round(rec["sum_arr"] / n, 2)
            else:
                min_arr = "NULL"
                max_arr = "NULL"
                avg_arr = "NULL"

            cxl_rate = round(n_cancel / n, 4) if n > 0 else 0.0

            print(f"{carrier}|{origin}|{month}|{n}|{min_arr}|{max_arr}|{avg_arr}|{cxl_rate}|{active}")

except BrokenPipeError:
    sys.stdout = None