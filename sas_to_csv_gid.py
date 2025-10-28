#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sas_to_csv_gid.py
-----------------
Lit (GID.sas + GID.txt) du PSID Family Identification Mapping System
et crée :
  - sorted_data/GID_full.csv
  - sorted_data/GID_full_labeled.csv
  - out/mapping_gid.csv
  - out/gid_labels.csv

Adapté au format :
   ERxxxxx   start - end
Exemple :
   ER30001   1 - 4
   ER30002   5 - 7
"""

from __future__ import annotations
import re, argparse
from pathlib import Path
from typing import List, Tuple, Dict
import pandas as pd

# ======================
# 1. Lecture du setup SAS
# ======================

# Capture les lignes du type "ER30001   1 - 4"
RANGE_RE = re.compile(r"([A-Za-z0-9_]+)\s+(\d+)\s*-\s*(\d+)")
LABEL_PAIR_RE = re.compile(r'([A-Za-z0-9_]+)\s+LABEL="([^"]+)"')

def parse_sas_schema(sas_path: Path) -> List[Tuple[str,int,int]]:
    text = sas_path.read_text(errors="ignore")
    schema = []
    for var, s, e in RANGE_RE.findall(text):
        schema.append((var.strip(), int(s), int(e)))
    if not schema:
        raise ValueError("Aucune plage 'var start - end' trouvée dans le SAS setup.")
    return schema

def parse_labels(sas_path: Path) -> Dict[str,str]:
    text = sas_path.read_text(errors="ignore")
    return {v.upper(): l for v, l in LABEL_PAIR_RE.findall(text)}

# ======================
# 2. Lecture FIXED-WIDTH
# ======================

def read_fixed_width(txt_path: Path, schema: List[Tuple[str,int,int]]) -> pd.DataFrame:
    colspecs = [(s-1, e) for (_, s, e) in schema]
    names = [n for (n,_,_) in schema]
    df = pd.read_fwf(txt_path, colspecs=colspecs, names=names, dtype=str)
    for c in df.columns:
        df[c] = df[c].astype("string").str.strip().replace({"": pd.NA})
    return df

# ======================
# 3. Mapping et dictionnaire
# ======================

HUMAN_DESCRIPTIONS = {
    "ER30001": "1968 INTERVIEW NUMBER (Family ID)",
    "ER30002": "PERSON NUMBER 68 (Individual ID)",
    "ER30001_P_F": "Father’s 1968 INTERVIEW NUMBER",
    "ER30002_P_F": "Father’s PERSON NUMBER 68",
    "ER30001_P_M": "Mother’s 1968 INTERVIEW NUMBER",
    "ER30002_P_M": "Mother’s PERSON NUMBER 68",
}

def build_mapping(schema: List[Tuple[str,int,int]], labels: Dict[str,str], source: str) -> pd.DataFrame:
    rows = []
    for name, s, e in schema:
        rows.append({
            "var_code": name,
            "start_pos": s,
            "end_pos": e,
            "label_sas": labels.get(name.upper(), ""),
            "label_description": HUMAN_DESCRIPTIONS.get(name.upper(), ""),
            "source": source
        })
    return pd.DataFrame(rows)

# ======================
# 4. Pipeline principal
# ======================

def run(sas_path: Path, txt_path: Path, out_csv: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    schema = parse_sas_schema(sas_path)
    labels = parse_labels(sas_path)
    print(f"[OK] Parsed {len(schema)} variables from {sas_path.name}")

    df = read_fixed_width(txt_path, schema)
    print(f"[OK] Loaded data from {txt_path.name} -> rows={len(df):,}, cols={df.shape[1]}")

    df.to_csv(out_csv, index=False)
    print(f"[OK] Wrote raw data -> {out_csv}")

    mapping = build_mapping(schema, labels, sas_path.name)
    mapping_path = out_dir / "mapping_gid.csv"
    mapping.to_csv(mapping_path, index=False)
    print(f"[OK] Wrote mapping -> {mapping_path}")

    dict_path = out_dir / "gid_labels.csv"
    mapping[["var_code", "label_sas", "label_description"]].to_csv(dict_path, index=False)
    print(f"[OK] Wrote dictionary -> {dict_path}")

    renamed = {c: f"{c} | {labels.get(c.upper(), HUMAN_DESCRIPTIONS.get(c.upper(), ''))}" for c in df.columns}
    df_labeled = df.rename(columns=renamed)
    out_labeled = out_csv.parent / "GID_full_labeled.csv"
    df_labeled.to_csv(out_labeled, index=False)
    print(f"[OK] Wrote labeled data -> {out_labeled}")

# ======================
# 5. CLI
# ======================

def main():
    ap = argparse.ArgumentParser(description="Convert GID.sas + GID.txt to clean CSV (fixed-width).")
    ap.add_argument("--sas", default="sorted_data/GID.sas")
    ap.add_argument("--txt", default="sorted_data/GID.txt")
    ap.add_argument("--out-csv", default="sorted_data/GID_full.csv")
    ap.add_argument("--out-dir", default="out")
    args = ap.parse_args()
    run(Path(args.sas), Path(args.txt), Path(args.out_csv), Path(args.out_dir))

if __name__ == "__main__":
    main()