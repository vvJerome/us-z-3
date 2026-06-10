"""Rewrite summary_counts.csv with per-Part Confirmed/Valid/Catch-All counts.

Hardcoded to the May 2026 NC/MI run. Replace with a manifest-driven version
once the manifest schema stabilises across states.
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT_CSV = ROOT / "output" / "vps_backup_20260521" / "us_output" / "summary_counts.csv"

PIPELINE: list[tuple[str, int, int, int, int]] = [
    ("Sara (w/ Officer)",            287_083,  45_051,   8_578,  36_473),
    ("Sara (Part 1) (w/o Officer)",  560_597, 181_745, 117_750,  63_995),
    ("Alpha (Part 2)",               436_700, 130_668, 104_683,  25_985),
    ("Alpha (Part 3)",               560_596, 229_893, 194_256,  35_637),
    ("Jerome (Part 4)",              560_597, 179_547, 163_932,  15_615),
    ("Jerome (Part 5)",              560_596, 185_413, 159_274,  26_139),
]

HEADER: list[str] = [
    "Source", "Unique Input Records",
    "Confirmed Emails", "Valid Emails", "Catch All",
]


def main() -> None:
    rows: list[list] = [list(p) for p in PIPELINE]
    total = ["TOTAL"] + [sum(r[i] for r in rows) for i in range(1, len(HEADER))]
    rows.append(total)

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)

    print(f"Wrote {OUT_CSV}")
    for r in rows:
        print(f"  {r[0]:40s}  input={r[1]:>9}  confirmed={r[2]:>7}  valid={r[3]:>7}  catch_all={r[4]:>7}")


if __name__ == "__main__":
    main()
