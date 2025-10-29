#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
filter_grid_rows.py
-------------------
Extracts rows (concepts) from canonical_grid.csv using permissive matching.

Features
- Modes: exact, contains, regex, fuzzy (with token-Jaccard + string ratio).
- Normalizes separators (/ _ -) and spaces; lowercases by default.
- Simple synonyms (ira <-> annuity, vehicles <-> vehicle, stocks <-> stock).
- Robust CSV reading with encoding fallbacks.
- Diagnostics: shows which requested concepts matched which grid concepts.
- Keeps the grid structure (years as columns), plus the last 'required' column if present.

Usage examples
--------------
# List all available concept strings
python filter_grid_rows.py --grid out/canonical_grid.csv --list

# Very permissive: contains + fuzzy fallbacks (default)
python filter_grid_rows.py --grid out/canonical_grid.csv \
  --concept "age" "sex" "marital status" "value vehicles" "value stocks" \
  --top 5 --out out/canonical_grid_study.csv

# Regex mode (advanced power)
python filter_grid_rows.py --grid out/canonical_grid.csv \
  --mode regex --concept "age$" "value\s+vehicles?" --out out/canonical_grid_study.csv
"""

from __future__ import annotations
import argparse
import csv
import re
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import pandas as pd

# Optional: use rapidfuzz if available (faster & better than difflib)
try:
    from rapidfuzz.fuzz import ratio as fuzz_ratio
except Exception:
    fuzz_ratio = None

# -------------------------
# Normalization & helpers
# -------------------------

SYNONYMS: Dict[str, str] = {
    "annuity/ira": "ira",
    "annuity ira": "ira",
    "annuities": "ira",
    "iras": "ira",
    "stock market": "stocks",
    "stock": "stocks",
    "vehicle": "vehicles",
    "vehicule": "vehicles",
    "home equity": "home_equity",
    "home-equity": "home_equity",
    "checking/saving": "checking_savings",
    "checking saving": "checking_savings",
    "checking_saving": "checking_savings",
    "cd/bonds/tb": "bonds",
}

SEP_RE = re.compile(r"[\/_\-]+")
WS_RE  = re.compile(r"\s+")

def norm(s: str, case_sensitive: bool = False) -> str:
    if not isinstance(s, str):
        s = str(s or "")
    s2 = s.strip()
    if not case_sensitive:
        s2 = s2.lower()
    # unify separators, then condense spaces
    s2 = SEP_RE.sub(" ", s2)
    s2 = WS_RE.sub(" ", s2).strip()
    # apply synonyms word-wise (very light touch)
    for k, v in SYNONYMS.items():
        s2 = re.sub(rf"\b{re.escape(k)}\b", v, s2)
    return s2

def token_set(s: str) -> set:
    return set(t for t in norm(s).split() if t)

def jaccard(a: str, b: str) -> float:
    A, B = token_set(a), token_set(b)
    if not A and not B:
        return 0.0
    inter = len(A & B)
    union = len(A | B)
    return inter / union if union else 0.0

def fuzzy(a: str, b: str) -> float:
    a2, b2 = norm(a), norm(b)
    if fuzz_ratio:
        return fuzz_ratio(a2, b2) / 100.0
    # fallback: naive token overlap ratio
    return jaccard(a2, b2)

# -------------------------
# Core selection logic
# -------------------------

def load_grid(path: Path) -> pd.DataFrame:
    # robust CSV reader
    tried = []
    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(path, dtype=str, engine="python")
        except Exception as e:
            tried.append(f"{enc}: {e}")
    raise RuntimeError("Failed to read CSV with common encodings:\n" + "\n".join(tried))

def list_concepts(grid: pd.DataFrame) -> List[str]:
    if "concept" not in grid.columns:
        # If the grid is indexed by concept (older runs), try index
        if grid.index.name == "concept" or "concept" not in grid.reset_index().columns:
            return [str(v) for v in grid.index.tolist()]
    return grid["concept"].astype(str).tolist()

def best_matches_for_query(
    query: str,
    candidates: List[str],
    mode: str,
    case_sensitive: bool,
    jac_threshold: float,
    fuzzy_threshold: float,
    top: int,
) -> List[Tuple[str, float, str]]:
    """
    Returns a list of (candidate, score, reason) sorted by score desc.
    """
    q_raw = query
    q = norm(query, case_sensitive)

    scored: List[Tuple[str, float, str]] = []

    for cand in candidates:
        c = norm(cand, case_sensitive)

        # exact
        if mode in {"exact", "auto"} and c == q:
            scored.append((cand, 1.0, "EXACT"))
            continue

        # regex
        if mode == "regex":
            try:
                if re.search(query, cand, 0 if case_sensitive else re.IGNORECASE):
                    scored.append((cand, 1.0, "REGEX"))
                continue
            except re.error:
                # fall back to auto if regex invalid
                pass

        # contains (normalized)
        if mode in {"contains", "auto"} and (q in c):
            # score by relative length to prefer tighter matches
            score = len(q) / max(1, len(c))
            scored.append((cand, score, "CONTAINS"))

        # token jaccard
        if mode in {"fuzzy", "auto"}:
            j = jaccard(q, c)
            if j >= jac_threshold:
                scored.append((cand, j, "JACCARD"))

        # string fuzzy
        if mode in {"fuzzy", "auto"}:
            f = fuzzy(q, c)
            if f >= fuzzy_threshold:
                scored.append((cand, f, "FUZZY"))

    # keep highest score per candidate
    best_by_cand: Dict[str, Tuple[float, str]] = {}
    for cand, score, why in scored:
        if cand not in best_by_cand or score > best_by_cand[cand][0]:
            best_by_cand[cand] = (score, why)

    ranked = sorted(((cand, s, why) for cand, (s, why) in best_by_cand.items()),
                    key=lambda x: x[1], reverse=True)
    return ranked[:top] if top else ranked

def select_rows(
    grid: pd.DataFrame,
    queries: List[str],
    mode: str,
    case_sensitive: bool,
    jac_threshold: float,
    fuzzy_threshold: float,
    top: int,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Returns: (selected_rows, diagnostics_lines, missing_queries)
    """
    concepts = list_concepts(grid)
    diag: List[str] = []
    selected_names: List[str] = []
    missing: List[str] = []

    for q in queries:
        matches = best_matches_for_query(
            q, concepts, mode, case_sensitive, jac_threshold, fuzzy_threshold, top
        )
        if not matches:
            missing.append(q)
            diag.append(f"[MISS] {q}")
            continue
        for cand, score, why in matches:
            diag.append(f"[{why} {score:.2f}] {q} -> {cand}")
            selected_names.append(cand)

    # unique and preserve order
    seen = set()
    ordered = [c for c in selected_names if not (c in seen or seen.add(c))]

    # Extract rows; grid may have concept as column or index
    if "concept" in grid.columns:
        out = grid[grid["concept"].astype(str).isin(ordered)].copy()
        # reorder in requested order
        out = out.set_index("concept").loc[ordered].reset_index()
    else:
        # assume index is concept
        grid_idx = grid.copy()
        grid_idx.index = grid_idx.index.astype(str)
        out = grid_idx.loc[[c for c in ordered if c in grid_idx.index]].copy()
        out = out.reset_index().rename(columns={"index": "concept"})

    return out, diag, missing

# -------------------------
# CLI
# -------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Permissive extractor for canonical_grid rows.")
    ap.add_argument("--grid", type=Path, default=Path("out/canonical_grid.csv"), help="Path to canonical_grid.csv")
    ap.add_argument("--out", type=Path, default=Path("out/canonical_grid_study.csv"), help="Output CSV path")
    ap.add_argument("--concept", nargs="+", default=None, help="Concepts to search (one or more).")
    ap.add_argument("--concepts-file", type=Path, default=None, help="Text file with one concept per line.")
    ap.add_argument("--mode", choices=["auto", "exact", "contains", "regex", "fuzzy"], default="auto",
                    help="Matching mode (auto tries all).")
    ap.add_argument("--jaccard", type=float, default=0.40, help="Token Jaccard threshold (0-1).")
    ap.add_argument("--fuzzy", type=float, default=0.60, help="Fuzzy ratio threshold (0-1).")
    ap.add_argument("--top", type=int, default=3, help="Max matches to keep per query (0 = keep all).")
    ap.add_argument("--case-sensitive", action="store_true", help="Do not lowercase for matching.")
    ap.add_argument("--list", action="store_true", help="List available concepts and exit.")
    ap.add_argument("--diagnostics", type=Path, default=None, help="Optional path to write diagnostics log.")
    return ap.parse_args()

def main():
    args = parse_args()
    out_path = args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    grid = load_grid(args.grid)

    if args.list:
        concepts = list_concepts(grid)
        for c in concepts:
            print(c)
        return

    # Collect queries
    queries: List[str] = []
    if args.concept:
        queries.extend(args.concept)
    if args.concepts_file and args.concepts_file.exists():
        queries.extend([line.strip() for line in args.concepts_file.read_text(encoding="utf-8").splitlines() if line.strip()])

    if not queries:
        print("[ERROR] No concepts provided. Use --concept … or --concepts-file FILE.")
        return

    selected, diag, missing = select_rows(
        grid=grid,
        queries=queries,
        mode=args.mode,
        case_sensitive=args.case_sensitive,
        jac_threshold=args.jaccard,
        fuzzy_threshold=args.fuzzy,
        top=args.top,
    )

    if selected.empty:
        print("[WARNING] No rows matched your queries.")
    else:
        selected.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"[OK] Wrote {out_path} ({len(selected)} rows)")

    # Write long version (melt) for quick scanning across years
    if not selected.empty:
        id_cols = ["concept"]
        value_cols = [c for c in selected.columns if c not in id_cols]
        long_df = selected.melt(id_vars=id_cols, value_vars=value_cols, var_name="year_or_col", value_name="var_code")
        long_out = out_path.with_name(out_path.stem + "_long.csv")
        long_df.to_csv(long_out, index=False)
        print(f"[OK] Wrote {long_out} ({len(long_df)} rows)")

    # Diagnostics
    if args.diagnostics:
        args.diagnostics.write_text("\n".join(diag), encoding="utf-8")
    else:
        for line in diag:
            print(line)
        if missing:
            print("[WARNING] Missing queries (no match):")
            for q in missing:
                print("  -", q)

if __name__ == "__main__":
    main()