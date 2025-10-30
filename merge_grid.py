#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_grid.py — interactive union-based merge (1 value per cell)
---------------------------------------------------------------

This script interactively merges rows in a canonical_grid.csv file.
For each group (1-based indices, separated by spaces), it computes the UNION
of values per column and writes a single value per cell (rules below).

⚙️ Merge rules:
- If no value → empty string
- If 1 value → keep it
- If >1 values → pick deterministically:
    1) prefer one already in the base row
    2) else pick the shortest, then lexicographically
  → shows [WARN] if multiple candidates exist

✅ 'concept' and 'required' columns are untouched.
✅ Only the first row of each group is updated.
✅ At the end, the script also writes a 'final_grid.csv' file containing only the merged rows.
"""

from __future__ import annotations
import argparse
import pandas as pd
import re
import sys
from pathlib import Path
from typing import List, Set

# -------------------------------------------------------------------

SEP_RE = re.compile(r"[|,;]\s*|\s{2,}")  # separators for cell values

def split_tokens(cell: str) -> Set[str]:
    """Split a cell into cleaned unique tokens."""
    if not isinstance(cell, str):
        return set()
    s = cell.strip()
    if not s:
        return set()
    if all(sep not in s for sep in ["|", ",", ";"]) and "  " not in s:
        return {s}
    parts = [p.strip() for p in SEP_RE.split(s) if p.strip()]
    return set(parts)

# -------------------------------------------------------------------

def read_groups_interactive() -> List[List[int]]:
    """Ask the user for row groups to merge (1-based indices)."""
    print("\n[INTERACTIVE] Enter groups of row numbers separated by spaces (1-based indices).")
    print("Example:  4 1969   or   3 1271 1272")
    print("Press Enter on an empty line to finish.\n")

    groups: List[List[int]] = []
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            break
        try:
            indices = [int(x) for x in line.split() if x.strip()]
        except ValueError:
            print(f"[ERROR] Invalid input: '{line}'")
            continue
        if len(indices) < 2:
            print(f"[WARN] Need at least 2 numbers; got '{line}'")
            continue
        groups.append(indices)
    return groups

# -------------------------------------------------------------------

def merge_by_union(df: pd.DataFrame, groups: List[List[int]]) -> (pd.DataFrame, pd.DataFrame):
    """Apply union-based merge for each group; return (merged_df, merged_rows_df)."""
    nrows = len(df)
    cols = [c for c in df.columns if c not in ("concept", "required")]

    warnings = 0
    changes = 0
    merged_indices = []  # store indices (0-based) of merged rows

    for grp in groups:
        out_of_range = [i for i in grp if i < 1 or i > nrows]
        if out_of_range:
            print(f"[WARN] Skipping group {grp}: indices out of range {out_of_range}")
            continue

        base_ix = grp[0] - 1
        merged_indices.append(base_ix)
        concept = df.at[base_ix, "concept"] if "concept" in df.columns else f"row {grp[0]}"

        for col in cols:
            base_tokens = split_tokens(str(df.iat[base_ix, df.columns.get_loc(col)]))
            all_tokens: Set[str] = set()
            for idx in grp:
                all_tokens |= split_tokens(str(df.iat[idx - 1, df.columns.get_loc(col)]))

            if len(all_tokens) == 0:
                new_val = ""
            elif len(all_tokens) == 1:
                new_val = next(iter(all_tokens))
            else:
                overlap = base_tokens & all_tokens
                if overlap:
                    new_val = sorted(overlap, key=lambda s: (len(s), s))[0]
                else:
                    new_val = sorted(all_tokens, key=lambda s: (len(s), s))[0]
                warnings += 1
                print(f"[WARN] Multiple values in '{col}' for '{concept}' -> {sorted(all_tokens)} (keeping '{new_val}')")

            old_val = str(df.iat[base_ix, df.columns.get_loc(col)]).strip()
            if old_val != new_val:
                df.iat[base_ix, df.columns.get_loc(col)] = new_val
                changes += 1

    print(f"\n[INFO] Processed {len(groups)} group(s); changed {changes} cell(s).")
    if warnings:
        print(f"[NOTE] {warnings} warning(s) for multi-valued unions.\n")

    # extract final merged rows
    merged_rows_df = df.iloc[sorted(set(merged_indices))].copy()
    return df, merged_rows_df

# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Interactive merge of canonical grid rows (union-based).")
    ap.add_argument("--file", required=True, type=Path, help="Path to canonical_grid.csv")
    ap.add_argument("--out", type=Path, default=None, help="Output file (default: canonical_grid_merged.csv)")
    args = ap.parse_args()

    if not args.file.exists():
        print(f"[ERROR] File not found: {args.file}")
        sys.exit(1)

    df = pd.read_csv(args.file, dtype=str, na_filter=False, keep_default_na=False)
    groups = read_groups_interactive()

    if not groups:
        print("[ERROR] No valid groups entered; exiting.")
        sys.exit(1)

    merged_df, merged_rows_df = merge_by_union(df, groups)

    # main merged grid
    out_path = args.out or args.file.with_name(args.file.stem + "_merged.csv")
    merged_df.to_csv(out_path, index=False)
    print(f"[OK] Wrote merged grid to {out_path}")

    # new CSV containing only merged rows
    final_path = args.file.parent / "final_grid.csv"
    merged_rows_df.to_csv(final_path, index=False)
    print(f"[OK] Wrote merged rows to {final_path}\n")

# -------------------------------------------------------------------

if __name__ == "__main__":
    main()