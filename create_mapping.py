#!/usr/bin/env python3
"""Build canonical mappings for PSID FAM/WLTH CSV extracts."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Tuple

import pandas as pd

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


def year_from_fname(path: Path) -> int:
    """Extract the first 4-digit year from a filename."""
    match = re.search(r"(\d{4})", path.name)
    if not match:
        raise ValueError(f"Could not parse year from {path.name}")
    return int(match.group(1))


def file_type_from_fname(path: Path) -> str | None:
    """Return the module type based on filename prefix."""
    name = path.name
    if WLTH_PREFIX_PATTERN.match(name):
        return "WLTH"
    if FAM_PREFIX_PATTERN.match(name):
        return "FAM"
    return None


def wlth_canonical_for(var_code: str) -> Tuple[str, str]:
    """Derive canonical name and category for a WLTH column."""
    code = var_code.strip()
    match = IRA_SUFFIX_PATTERN.search(code)
    if match:
        tail, alias = match.groups()
        base = IRA_TAIL_MAP[tail]
        if alias:
            base = f"{base}_A"
        return base, "Retirement/IRA"
    return f"wlth_{code.lower()}", "Assets/Debt"


def fam_canonical_for(var_code: str) -> Tuple[str, str]:
    """Derive canonical name and category for a FAM column."""
    code = var_code.strip()
    upper = code.upper()
    if upper == "FEMALE":
        return "sex_head_female", "Demographics"
    if upper == "CHILD":
        return "num_children", "Demographics"
    if upper.startswith("HAD_"):
        return "head_presence_flag", "Demographics"
    return f"fam_{code.lower()}", "FAM/Unknown"


def build_mapping(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scan CSV headers and build mapping/inventory DataFrames."""
    mapping_rows = []
    inventory_rows = []

    for csv_path in sorted(data_dir.glob("*.csv")):
        file_type = file_type_from_fname(csv_path)
        if file_type is None:
            continue

        year = year_from_fname(csv_path)
        header_df = pd.read_csv(csv_path, nrows=0)

        for var_code in header_df.columns:
            canonical, category = (
                wlth_canonical_for(var_code)
                if file_type == "WLTH"
                else fam_canonical_for(var_code)
            )
            mapping_rows.append(
                {
                    "canonical": canonical,
                    "year": year,
                    "file_type": file_type,
                    "var_code": var_code,
                    "label": "",
                    "category": category,
                    "dtype": "",
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
            "category",
            "dtype",
            "required",
            "transform",
        ],
    )
    if not mapping_df.empty:
        mapping_df = mapping_df.sort_values(["year", "file_type", "canonical", "var_code"]).reset_index(
            drop=True
        )

    inventory_df = pd.DataFrame(inventory_rows, columns=["file", "year", "var_code"])
    if not inventory_df.empty:
        inventory_df = inventory_df.sort_values(["year", "file", "var_code"]).reset_index(drop=True)

    return mapping_df, inventory_df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create canonical mapping and inventory for PSID FAM/WLTH extracts."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory containing FAM/WLTH CSV files. Defaults to current directory.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path.cwd(),
        help="Destination directory for outputs. Defaults to current directory.",
    )

    args = parser.parse_args()
    data_dir = args.data_dir.resolve()
    out_dir = args.out_dir.resolve()

    if not data_dir.exists() or not data_dir.is_dir():
        raise FileNotFoundError(f"--data-dir {data_dir} is not a directory")

    mapping_df, inventory_df = build_mapping(data_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = out_dir / "mapping_long.csv"
    inventory_path = out_dir / "fam_wlth_inventory.csv"

    mapping_df.to_csv(mapping_path, index=False)
    inventory_df.to_csv(inventory_path, index=False)

    print(f"[OK] wrote {mapping_path.name} ({len(mapping_df)} rows)")
    print(f"[OK] wrote {inventory_path.name} ({len(inventory_df)} rows)")


if __name__ == "__main__":
    main()
