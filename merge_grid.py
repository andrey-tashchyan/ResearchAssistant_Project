#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge_grid.py — union-based merge (1 value per cell)
----------------------------------------------------
This script takes a canonical_grid.csv and groups of row numbers to merge.
For each group (1-based indices), it computes the UNION of values in each
year column (splitting cells by separators like '|', ',', ';', whitespace),
and writes a SINGLE value into the first row of the group:

- If the union is empty -> writes empty string.
- If the union has exactly 1 value -> writes that value.
- If the union has >1 values -> emits a WARNING and picks one deterministically:
    1) Prefer a value already present in the base row (if any).
    2) Otherwise pick the shortest string; tie-break lexicographically.

The 'concept' column is never changed. The 'required' column (if present) is
left untouched (not merged). Source rows are not altered.

USAGE (CLI):
    python merge_grid.py --file out/canonical_grid.csv \
        --groups "4 7 8, 13 6706" \
        --out out/canonical_grid_merged.csv

USAGE (Interactive):
    python merge_grid.py --file out/canonical_grid.csv --interactive --out out/canonical_grid_merged.csv
    Then type one group per line, numbers separated by spaces:
        4 7 8
        13 6706
        <empty line to finish>

Notes:
- Groups are 1-based row numbers referring to the CSV after load (first data row is 1).
- In --groups, groups are separated by commas; inside a group you may separate numbers
  by spaces or dots (e.g., "4 7 8" or "4.7.8").
"""

from __future__ import annotations
import argparse
import pandas as pd
import sys
import re
from pathlib import Path
from typing import List, Set

# -------------------------- Parsing helpers --------------------------

def parse_groups(groups_arg: str) -> List[List[int]]:
    """
    Parse a string like: "4 7 8, 13 6706" or "4.7.8,13.6706" -> [[4,7,8], [13,6706]]
    """
    groups: List[List[int]] = []
    if not groups_arg:
        return groups
    for chunk in groups_arg.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            indices = [int(x) for x in re.split(r"[.\s]+", chunk) if x.strip()]
        except ValueError:
            print(f"[ERROR] Invalid group format: '{chunk}'", file=sys.stderr)
            continue
        if len(indices) >= 2:
            groups.append(indices)
        else:
            print(f"[WARN] Ignoring group with <2 rows: '{chunk}'")
    return groups


def read_groups_interactive() -> List[List[int]]:
    """
    Interactive prompt: one group per line, numbers separated by spaces.
    Empty line to finish.
    """
    print("[INTERACTIVE] Enter groups of 1-based row numbers separated by spaces (e.g., '4 7 8').")
    print("[INTERACTIVE] Press Enter on an empty line to finish.")
    groups: List[List[int]] = []
    while True:
        line = input("> ").strip()
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

# -------------------------- Token helpers ----------------------------

SEP_RE = re.compile(r"[|,;]\s*|\s{2,}")  # split on | , ; or long whitespace (keeps single spaces in codes intact)

def split_tokens(cell: str) -> Set[str]:
    """
    Split a cell into tokens, using separators | , ; and runs of 2+ spaces.
    We intentionally *keep* single spaces inside codes (e.g., 'ER 30001').
    """
    if not isinstance(cell, str):
        return set()
    s = cell.strip()
    if not s:
        return set()
    # quick paths for simple single-value cells
    if all(sep not in s for sep in ["|", ",", ";"]) and "  " not in s:
        return {s}
    parts = [p.strip() for p in SEP_RE.split(s) if p.strip()]
    return set(parts)

# -------------------------- Merge core --------------------------------

def merge_by_union(df: pd.DataFrame, groups: List[List[int]]) -> pd.DataFrame:
    """
    For each group:
      - compute union of tokens per column (excluding 'concept' and 'required')
      - write 1 value into the base row: prefer a pre-existing base token; else shortest then lexicographic
      - warn if union has >1 candidates
    """
    nrows = len(df)
    columns_to_merge = [c for c in df.columns if c not in ("concept", "required")]
    warnings = 0
    changed_cells = 0
    processed_groups = 0

    for grp in groups:
        # bounds check (1-based to 0-based)
        out_of_range = [i for i in grp if i < 1 or i > nrows]
        if out_of_range:
            print(f"[WARN] Skipping group {grp}: indices out of range {out_of_range}")
            continue

        base_ix = grp[0] - 1
        concept_name = str(df.at[base_ix, "concept"]) if "concept" in df.columns else f"row {grp[0]}"

        for col in columns_to_merge:
            # collect union across all rows in the group
            union_vals: Set[str] = set()
            base_tokens: Set[str] = split_tokens(str(df.iat[base_ix, df.columns.get_loc(col)]))

            for one_based in grp:
                cell = str(df.iat[one_based - 1, df.columns.get_loc(col)])
                union_vals |= split_tokens(cell)

            if len(union_vals) == 0:
                new_val = ""
            elif len(union_vals) == 1:
                new_val = next(iter(union_vals))
            else:
                # prefer keeping a token that already exists in the base cell
                candidate = None
                base_overlap = base_tokens & union_vals
                if base_overlap:
                    # deterministic pick among overlaps: shortest then lexicographic
                    candidate = sorted(base_overlap, key=lambda s: (len(s), s))[0]
                else:
                    candidate = sorted(union_vals, key=lambda s: (len(s), s))[0]
                new_val = candidate
                warnings += 1
                print(f"[WARN] Multiple values in union for column '{col}' / concept '{concept_name}' "
                      f"({len(union_vals)} values): {sorted(union_vals)} -> keeping '{new_val}'")

            old_val = str(df.iat[base_ix, df.columns.get_loc(col)]).strip()
            if old_val != new_val:
                df.iat[base_ix, df.columns.get_loc(col)] = new_val
                changed_cells += 1

        processed_groups += 1

    print(f"[INFO] Processed {processed_groups} group(s); changed {changed_cells} cell(s).")
    if warnings:
        print(f"[NOTE] {warnings} warning(s) for multi-valued unions.")
    return df

# -------------------------- CLI ---------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Merge canonical grid rows by UNION of cell values (one value per cell).")
    ap.add_argument("--file", type=Path, required=True, help="Path to canonical_grid.csv")
    ap.add_argument("--groups", type=str, default=None,
                    help='Groups like "4 7 8, 13 6706" (1-based). Inside each group, numbers can be separated by spaces or dots.')
    ap.add_argument("--interactive", action="store_true", help="Interactive mode to input groups line-by-line")
    ap.add_argument("--out", type=Path, default=None, help="Output CSV (default: canonical_grid_merged.csv next to input)")
    args = ap.parse_args()

    in_path: Path = args.file
    if not in_path.exists():
        print(f"[ERROR] File not found: {in_path}")
        sys.exit(1)

    df = pd.read_csv(in_path, dtype=str, na_filter=False, keep_default_na=False)

    if args.interactive:
        groups = read_groups_interactive()
    else:
        if not args.groups:
            print("[ERROR] You must pass --groups or use --interactive.")
            sys.exit(1)
        groups = parse_groups(args.groups)

    if not groups:
        print("[ERROR] No valid groups provided.")
        sys.exit(1)

    print(f"[INFO] Merging {len(groups)} group(s) by UNION...")
    merged_df = merge_by_union(df, groups)

    out_path = args.out or (in_path.parent / "canonical_grid_merged.csv")
    merged_df.to_csv(out_path, index=False)
    print(f"[OK] Wrote merged grid to {out_path}")

if __name__ == "__main__":
    main()