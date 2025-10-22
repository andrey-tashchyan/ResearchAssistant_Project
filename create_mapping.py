#!/usr/bin/env python3
"""Build canonical mappings for PSID FAM/WLTH CSV extracts, reading labels from the 2nd line of each *_full.csv."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Tuple, Dict, Optional

import pandas as pd
import numpy as np

FAM_PREFIX_PATTERN = re.compile(r"^FAM\d{4}", re.IGNORECASE)
WLTH_PREFIX_PATTERN = re.compile(r"^WLTH\d{4}", re.IGNORECASE)
IRA_SUFFIX_PATTERN = re.compile(r"(1[3-9]|20)(A?)$")
IRA_TAIL_MAP = {
    "13": "ira_any",
    "14": "ira_num",
    "15": "ira_balance",
    "16": "ira_contrib",
    "17": "ira_withdrawal",
    "18": "ira_type",
    "19": "ira_aux1",
    "20": "ira_aux2",
}

# ------------ helpers: filename -> year/module ------------

def year_from_fname(path: Path) -> int:
    m = re.search(r"(\d{4})", path.name)
    if not m:
        raise ValueError(f"Could not parse year from {path.name}")
    return int(m.group(1))

def file_type_from_fname(path: Path) -> Optional[str]:
    n = path.name
    if WLTH_PREFIX_PATTERN.match(n):
        return "WLTH"
    if FAM_PREFIX_PATTERN.match(n):
        return "FAM"
    return None

# ------------ canonical rules (simplified; adjust as needed) ------------

def wlth_canonical_for(var_code: str) -> Tuple[str, str]:
    code = var_code.strip()
    m = IRA_SUFFIX_PATTERN.search(code)
    if m:
        tail, alias = m.groups()
        base = IRA_TAIL_MAP[tail]
        if alias:
            base = f"{base}_A"
        return base, "Retirement/IRA"
    return f"wlth_{code.lower()}", "Assets/Debt"

def fam_canonical_for(var_code: str) -> Tuple[str, str]:
    code = var_code.strip()
    up = code.upper()
    if up == "FEMALE":
        return "sex_head_female", "Demographics"
    if up == "CHILD":
        return "num_children", "Demographics"
    if up.startswith("HAD_"):
        return "head_presence_flag", "Demographics"
    return f"fam_{code.lower()}", "FAM/Unknown"

# ------------ CSV second-row labels ------------

def read_second_row_labels(csv_path: Path) -> Optional[pd.Series]:
    """
    PSID *_full.csv convention for this project:
      - header row: variable codes
      - FIRST data row (second physical line): human-readable labels
    Return that first data row as a Series of labels, or None if not present.
    """
    try:
        df2 = pd.read_csv(csv_path, nrows=2, dtype=str, low_memory=False)
    except Exception:
        return None
    if df2.empty:
        return None
    row0 = df2.iloc[0].astype(str).str.strip()
    return row0

# ------------ dtype guess ------------

def guess_dtype(csv_path: Path, skip_first_data_row: bool) -> Dict[str, str]:
    """
    Guess a light dtype (numeric vs string) on a small sample.
    We skip the first data row because it contains labels.
    """
    kw = dict(dtype=str, low_memory=False)
    if skip_first_data_row:
        kw["skiprows"] = [1]
    try:
        sample = pd.read_csv(csv_path, nrows=500, **kw)
    except Exception:
        return {}
    dtypes: Dict[str, str] = {}
    for col in sample.columns:
        s = pd.to_numeric(sample[col], errors="coerce")
        num_frac = s.notna().mean()
        dtypes[col] = "float64" if num_frac > 0.9 else "string"
    return dtypes

# ------------ core mapping build ------------

def build_mapping(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mapping_rows = []
    inventory_rows = []

    # list CSVs sorted for determinism
    for csv_path in sorted(data_dir.glob("*.csv")):
        file_type = file_type_from_fname(csv_path)
        if file_type is None:
            continue

        year = year_from_fname(csv_path)

        # labels from the SECOND physical line (first data row)
        label_row_series = read_second_row_labels(csv_path)
        has_label_row = label_row_series is not None

        # header (variable codes)
        header_df = pd.read_csv(csv_path, nrows=0)
        columns = list(header_df.columns)

        # dtype guess (ignore label row)
        dtype_guess = guess_dtype(csv_path, skip_first_data_row=True)

        for var_code in columns:
            up = var_code.upper()
            # label from second row; source is the CSV file name
            label = ""
            label_source = csv_path.name
            if has_label_row and var_code in label_row_series.index:
                cand = str(label_row_series[var_code])
                label = cand.strip() if cand is not None else ""

            # canonical/category
            canonical, category = (
                wlth_canonical_for(var_code) if file_type == "WLTH" else fam_canonical_for(var_code)
            )

            mapping_rows.append(
                {
                    "canonical": canonical,
                    "year": year,
                    "file_type": file_type,
                    "var_code": var_code,
                    "label": label,
                    "label_source": label_source,   # <= NOM DU FICHIER FULL.CSV
                    "category": category,
                    "dtype": dtype_guess.get(var_code, ""),
                    "required": 0,
                    "transform": "",
                }
            )
            inventory_rows.append(
                {
                    "file": csv_path.name,
                    "year": year,
                    "var_code": var_code,
                }
            )

    mapping_df = pd.DataFrame(
        mapping_rows,
        columns=[
            "canonical",
            "year",
            "file_type",
            "var_code",
            "label",
            "label_source",
            "category",
            "dtype",
            "required",
            "transform",
        ],
    )
    if not mapping_df.empty:
        mapping_df = mapping_df.sort_values(["year", "file_type", "canonical", "var_code"]).reset_index(drop=True)

    inventory_df = pd.DataFrame(inventory_rows, columns=["file", "year", "var_code"])
    if not inventory_df.empty:
        inventory_df = inventory_df.sort_values(["year", "file", "var_code"]).reset_index(drop=True)

    return mapping_df, inventory_df

# ------------ CLI ------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create canonical mapping and inventory for PSID FAM/WLTH extracts (labels from CSV 2nd line)."
    )
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(),
                        help="Directory containing FAM/WLTH CSV files.")
    parser.add_argument("--out-dir", type=Path, default=Path.cwd(),
                        help="Destination directory for outputs.")
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    out_dir = args.out_dir.resolve()
    if not data_dir.exists() or not data_dir.is_dir():
        raise FileNotFoundError(f"--data-dir {data_dir} is not a directory")

    mapping_df, inventory_df = build_mapping(data_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "mapping_long.csv").write_text("")  # ensure create/overwrite cleanly
    mapping_df.to_csv(out_dir / "mapping_long.csv", index=False)
    inventory_df.to_csv(out_dir / "fam_wlth_inventory.csv", index=False)

    print(f"[OK] wrote mapping_long.csv ({len(mapping_df)} rows)")
    print(f"[OK] wrote fam_wlth_inventory.csv ({len(inventory_df)} rows)")

if __name__ == "__main__":
    main()