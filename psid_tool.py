#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
make_canonical_grid_min.py — build canonical_grid.csv only (with row numbers)
-----------------------------------------------------------------------------
Reads mapping_long.csv, resolves conflicts, pivots to concept × years -> var_code,
and writes canonical_grid.csv with a leading 'row' column (1-based).
"""

from __future__ import annotations
import argparse
from pathlib import Path
import re
import unicodedata
from typing import Dict, Optional, Set
import pandas as pd

# -------------------- Utils --------------------

MODULE_PREFIXES = {"FAM", "WLTH"}
WS_RE        = re.compile(r"\s+")
WAVE_RE      = re.compile(r"\(W\d+\)", re.IGNORECASE)
YEAR2_RE     = re.compile(r"\b\d{2}\b")
YEAR4_RE     = re.compile(r"\b(19|20)\d{2}\b")
PUNCT_RE     = re.compile(r"[^\w\s/]+")
BRACKETS_RE  = re.compile(r"[\[\]\(\)\{\}]")
DIGIT_TOK_RE = re.compile(r"\b\d+\b")

STOP_TOKENS = {
    "imp","acc","wtr","whether","ever","any","of","the","a","an","and","or","to","in","for","by","head","hh","household"
}

SYNONYMS: Dict[str, str] = {
    "annuity/ira": "ira",
    "iras": "ira",
    "stock market": "stocks",
    "stock": "stocks",
    "wealth without equity": "wealth_wo_equity",
    "wealth w/o equity": "wealth_wo_equity",
    "home equity": "home_equity",
    "other asset": "other_assets",
    "other assets": "other_assets",
    "vehicle": "vehicles",
    "vehicles": "vehicles",
    "balance": "value",
    "account": "acct",
    "accounts": "acct",
    "mortgages": "mortgage",
}

def normalize_header(name: str) -> str:
    return unicodedata.normalize("NFC", str(name)).strip()

def parse_year_filters(arg: Optional[str]) -> Optional[Set[int]]:
    if not arg:
        return None
    years: Set[int] = set()
    for piece in arg.split(","):
        p = piece.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            try:
                a, b = int(a), int(b)
            except ValueError:
                continue
            if a > b:
                a, b = b, a
            years.update(range(a, b + 1))
        else:
            try:
                years.add(int(p))
            except ValueError:
                pass
    return years

def normalize_phrase(s: str) -> str:
    s = s.lower().strip()
    s = WAVE_RE.sub(" ", s)
    s = YEAR2_RE.sub(" ", s)
    s = YEAR4_RE.sub(" ", s)
    s = BRACKETS_RE.sub(" ", s)
    s = PUNCT_RE.sub(" ", s)
    s = WS_RE.sub(" ", s)
    return s.strip()

def apply_synonyms(s: str) -> str:
    out = s
    for k, v in SYNONYMS.items():
        out = re.sub(rf"\b{re.escape(k)}\b", v, out)
    return WS_RE.sub(" ", out).strip()

def label_to_concept(label: str) -> str:
    if not isinstance(label, str) or not label.strip():
        return ""
    s = normalize_phrase(label)
    toks = []
    for t in s.split():
        if t in STOP_TOKENS:
            continue
        if DIGIT_TOK_RE.fullmatch(t):
            continue
        toks.append(t)
    s2 = " ".join(toks)
    s2 = apply_synonyms(s2)
    s2 = re.sub(r"\bvalue of\b", "value", s2)
    return WS_RE.sub(" ", s2).strip()

def score_row(row: pd.Series, prefer_module: str, code_freq: Dict[str, int]) -> int:
    label = str(row.get("label", "")).lower()
    var   = str(row.get("var_code", ""))
    mod   = str(row.get("file_type", "")).upper()
    score = 0
    if "acc" in f" {label} ": score += 3
    if "imp" in f" {label} ": score -= 3
    if var.endswith("A"): score += 2
    if "value" in label: score += 1
    if ("whether" in label) or (" wtr " in f" {label} "): score -= 1
    if prefer_module in {"WLTH","FAM"} and mod == prefer_module: score += 1
    score += min(len(label)//40, 2)
    score += min(code_freq.get(var, 0), 2)
    return score

# -------------------- Core --------------------

def build_canonical_grid(
    mapping_csv: Path,
    out_dir: Path,
    prefer: str = "WLTH",
    drop_imp: bool = False,
    years: Optional[Set[int]] = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read mapping
    df = pd.read_csv(mapping_csv, dtype=str)
    for c in ["year", "var_code", "label"]:
        if c not in df.columns:
            raise ValueError(f"Missing required column '{c}' in {mapping_csv}")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype("int64")
    df["var_code"] = df["var_code"].apply(normalize_header)
    df["label"] = df["label"].fillna("").astype(str)
    df["file_type"] = df.get("file_type", "UNK").astype(str).str.upper()
    if "category" not in df.columns:
        df["category"] = ""

    # Filter years
    if years:
        df = df[df["year"].isin(years)].copy()

    # Drop IMP if requested
    if drop_imp:
        df = df[~df["label"].str.contains(r"\bIMP\b", case=False, na=False)].copy()

    # Build concept
    df["concept_base"] = df["label"].apply(label_to_concept)
    df = df[df["concept_base"].str.len() >= 3].copy()
    if "category" in df.columns:
        df["concept"] = (df["category"].fillna("").str.lower().str.strip() + " :: " + df["concept_base"])
    else:
        df["concept"] = df["concept_base"]

    # Score & tie-break
    code_freq = df["var_code"].value_counts().to_dict()
    df["score"] = df.apply(lambda r: score_row(r, prefer, code_freq), axis=1)
    df["label_len"] = df["label"].str.len()
    df = df.sort_values(
        by=["concept", "year", "score", "file_type", "label_len", "var_code"],
        ascending=[True, True, False, True, False, True]
    )
    picked = (df.groupby(["concept", "year"], as_index=False)
                .head(1)
                .reset_index(drop=True))

    # Pivot -> grid
    grid = picked.pivot(index="concept", columns="year", values="var_code")
    try:
        grid = grid.reindex(sorted(grid.columns, key=lambda x: int(str(x))), axis=1)
    except Exception:
        grid = grid.sort_index(axis=1)

    # Add row numbers as first column (1-based)
    grid_out = grid.reset_index()
    grid_out.insert(0, "row", range(1, len(grid_out) + 1))

    out_path = out_dir / "canonical_grid.csv"
    grid_out.to_csv(out_path, index=False)
    print(f"[OK] Wrote {out_path} (rows={grid_out.shape[0]}, years={grid.shape[1]})")
    return out_path

# -------------------- CLI --------------------

def parse_year_filters(arg: Optional[str]) -> Optional[Set[int]]:
    if not arg:
        return None
    years: Set[int] = set()
    for piece in arg.split(","):
        p = piece.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            try:
                a, b = int(a), int(b)
            except ValueError:
                continue
            if a > b:
                a, b = b, a
            years.update(range(a, b + 1))
        else:
            try:
                years.add(int(p))
            except ValueError:
                pass
    return years

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build canonical_grid.csv from mapping_long.csv (only), with row numbers.")
    ap.add_argument("--mapping", type=Path, default=Path("out/mapping_long.csv"))
    ap.add_argument("--out-dir", type=Path, default=Path("out"))
    ap.add_argument("--prefer", default="WLTH", choices=["WLTH", "FAM"])
    ap.add_argument("--drop-imp", action="store_true")
    ap.add_argument("--years", default=None, help="Comma list or ranges (e.g. 1999,2001-2005)")
    return ap.parse_args()

def main():
    args = parse_args()
    years = parse_year_filters(args.years)
    build_canonical_grid(
        mapping_csv=args.mapping,
        out_dir=args.out_dir,
        prefer=args.prefer,
        drop_imp=args.drop_imp,
        years=years,
    )

if __name__ == "__main__":
    main()