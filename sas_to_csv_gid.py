#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convertit un couple (GID.sas + GID.txt) du PSID Family Identification Mapping System
en un CSV lisible, sans dépendance SAS externe.
"""

import re
import pandas as pd
from pathlib import Path

# ======================
# 1. Extraction du schéma SAS
# ======================

def parse_sas_schema(sas_path):
    sas_text = Path(sas_path).read_text(errors="ignore")
    # capture des lignes INPUT avec pointeurs @
    pattern = re.compile(r"@(\d+)\s+([A-Za-z0-9_]+)\s+(\$?\d+)\.", re.IGNORECASE)
    matches = pattern.findall(sas_text)

    variables = []
    for start, name, length in matches:
        start = int(start)
        length = int(length.replace("$",""))
        end = start + length - 1
        var_type = "string" if "$" in length else "numeric"
        variables.append((name, start, end, var_type))
    return variables


# ======================
# 2. Lecture du fichier ASCII
# ======================

def read_ascii_with_schema(txt_path, schema):
    colspecs = [(v[1]-1, v[2]) for v in schema]
    names = [v[0] for v in schema]
    df = pd.read_fwf(txt_path, colspecs=colspecs, names=names, dtype=str)
    df = df.apply(lambda c: c.str.strip())
    return df


# ======================
# 3. Conversion principale
# ======================

def sas_to_csv_gid(sas_path, txt_path, out_path):
    schema = parse_sas_schema(sas_path)
    print(f"[INFO] Parsed {len(schema)} variables from {sas_path}")
    df = read_ascii_with_schema(txt_path, schema)
    print(f"[INFO] Read {len(df):,} rows from {txt_path}")
    df.to_csv(out_path, index=False)
    print(f"[OK] Wrote {out_path}")


# ======================
# 4. Point d’entrée
# ======================

if __name__ == "__main__":
    sas_path = "sorted_data/GID.sas"
    txt_path = "sorted_data/GID.txt"
    out_path = "sorted_data/GID_full.csv"
    sas_to_csv_gid(sas_path, txt_path, out_path)