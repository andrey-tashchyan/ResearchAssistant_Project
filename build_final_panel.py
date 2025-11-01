#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_final_panel.py — Memory-efficient PSID panel builder with Parquet output
================================================================================
Instrumented for maximum observability.
"""

from __future__ import annotations

import argparse
import gc
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None

# Enable copy-on-write
pd.options.mode.copy_on_write = True

# ============================================================================
# Color Helpers
# ============================================================================

COLORS = {
    'CYAN': '\033[0;36m',
    'MAGENTA': '\033[0;35m',
    'GREEN': '\033[0;32m',
    'BLUE': '\033[0;34m',
    'YELLOW': '\033[0;33m',
    'RED': '\033[0;31m',
    'GRAY': '\033[0;90m',
    'RESET': '\033[0m'
}

def c(msg: str, color: str) -> str:
    """Colorize message."""
    return f"{COLORS.get(color, '')}{msg}{COLORS['RESET']}"

def log_title(msg: str): print(c(f"[TITLE] {msg}", 'CYAN'))
def log_step(msg: str): print(c(f"[STEP] {msg}", 'MAGENTA'))
def log_info(msg: str): print(c(f"[INFO] {msg}", 'GREEN'))
def log_file(msg: str): print(c(f"[FILE] {msg}", 'GRAY'))
def log_read(msg: str): print(c(f"[READ] {msg}", 'GREEN'))
def log_write(msg: str): print(c(f"[WRITE] {msg}", 'GREEN'))
def log_skip(msg: str): print(c(f"[SKIP] {msg}", 'YELLOW'))
def log_warn(msg: str): print(c(f"[WARN] {msg}", 'YELLOW'))
def log_ok(msg: str): print(c(f"[OK] {msg}", 'GREEN'))
def log_done(msg: str): print(c(f"[DONE] {msg}", 'GREEN'))
def log_time(msg: str): print(c(f"[TIME] {msg}", 'BLUE'))
def log_error(msg: str): print(c(f"[ERROR] {msg}", 'RED'), file=sys.stderr)


# ============================================================================
# Type Optimization
# ============================================================================

def downcast_column(series: pd.Series, target_type: str) -> pd.Series:
    """Aggressively downcast a column."""
    try:
        if target_type == "int32":
            numeric = pd.to_numeric(series, errors="coerce")
            return numeric.astype("Int32")
        elif target_type == "float32":
            numeric = pd.to_numeric(series, errors="coerce")
            return numeric.astype("float32")
        elif target_type == "category":
            if series.nunique() < len(series) * 0.5:
                return series.astype("category")
            else:
                return series.astype("string")
        elif target_type == "string":
            return series.astype("string")
        else:
            return series
    except Exception as e:
        log_warn(f"Column '{series.name}' downcast to {target_type} failed: {e}, fallback to string")
        return series.astype("string")


def infer_and_downcast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Infer types and downcast all columns."""
    cast_log = []

    for col in df.columns:
        if col in {"year", "module"}:
            continue

        series = df[col]
        original_dtype = str(series.dtype)

        if series.dtype in ["Int32", "float32", "category", "string"]:
            continue

        # Try numeric first
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = numeric.notna().sum()

        if non_null > len(series) * 0.8:
            if numeric.dropna().apply(lambda x: x == int(x)).all():
                df[col] = downcast_column(series, "int32")
                cast_log.append(f"{col}: {original_dtype}→Int32")
            else:
                df[col] = downcast_column(series, "float32")
                cast_log.append(f"{col}: {original_dtype}→float32")
        else:
            df[col] = downcast_column(series, "category")
            cast_log.append(f"{col}: {original_dtype}→category/string")

    if cast_log:
        sample = ", ".join(cast_log[:5])
        if len(cast_log) > 5:
            sample += f" ... ({len(cast_log)} total)"
        log_info(f"Type casts: {sample}")

    return df


# ============================================================================
# File Discovery
# ============================================================================

def discover_files(data_dir: Path) -> Dict[Tuple[int, str], Path]:
    """Discover FAM/WLTH CSV files."""
    log_step("Discovering data files...")
    t0 = time.perf_counter()

    files_map: Dict[Tuple[int, str], Path] = {}

    # FAM files
    for fam_file in sorted(data_dir.glob("FAM*ER_full.csv")):
        try:
            year = int(fam_file.stem[3:7])
            files_map[(year, "FAM")] = fam_file
            log_file(f"Found FAM{year}: {fam_file.name}")
        except ValueError:
            log_warn(f"Could not parse year from: {fam_file.name}")

    # WLTH files
    for wlth_file in sorted(data_dir.glob("WLTH*_full.csv")):
        try:
            year = int(wlth_file.stem[4:8])
            files_map[(year, "WLTH")] = wlth_file
            log_file(f"Found WLTH{year}: {wlth_file.name}")
        except ValueError:
            log_warn(f"Could not parse year from: {wlth_file.name}")

    elapsed = time.perf_counter() - t0
    log_ok(f"Discovered {len(files_map)} files across {len(set(y for y, _ in files_map))} years")
    log_time(f"File discovery: {elapsed:.3f}s")

    return files_map


# ============================================================================
# Mapping Processing
# ============================================================================

def load_canonical_grid(grid_path: Path) -> Set[str]:
    """Load final_grid.csv and extract canonical variable names."""
    log_step(f"Loading canonical grid...")
    t0 = time.perf_counter()

    if not grid_path.exists():
        log_error(f"Grid file not found: {grid_path}")
        sys.exit(1)

    log_read(f"Reading: {grid_path}")
    grid_df = pd.read_csv(grid_path, nrows=0)

    canonical_cols = set(grid_df.columns)
    canonical_cols.discard("row")
    canonical_cols.discard("concept")

    elapsed = time.perf_counter() - t0
    log_ok(f"Loaded {len(canonical_cols)} canonical variables")
    log_time(f"Grid load: {elapsed:.3f}s")

    return canonical_cols


def build_chunk_mappings(
    mapping_path: Path,
    canonical_vars: Set[str]
) -> Dict[Tuple[int, str], Tuple[List[str], Dict[str, str]]]:
    """Build per-(year, module) mappings."""
    log_step("Building chunk mappings from mapping_long.csv...")
    t0 = time.perf_counter()

    if not mapping_path.exists():
        log_error(f"Mapping file not found: {mapping_path}")
        sys.exit(1)

    log_read(f"Reading: {mapping_path}")
    mapping_df = pd.read_csv(
        mapping_path,
        dtype={
            "year": "int32",
            "var_code": "string",
            "label": "string",
            "file_type": "category"
        },
        usecols=["year", "var_code", "label", "file_type"]
    )

    log_info(f"Loaded {len(mapping_df):,} mapping rows")

    mapping_df["canonical"] = mapping_df["label"].str.lower().str.strip()
    mapping_filtered = mapping_df[mapping_df["var_code"].notna()].copy()

    log_info(f"Filtered to {len(mapping_filtered):,} rows with valid var_codes")

    chunk_maps: Dict[Tuple[int, str], Tuple[List[str], Dict[str, str]]] = {}

    groups = list(mapping_filtered.groupby(["year", "file_type"]))

    iterator = tqdm(groups, desc="Building mappings", unit="chunk") if HAS_TQDM else groups

    for (year, module), group in iterator:
        usecols = group["var_code"].unique().tolist()
        rename_map = dict(zip(group["var_code"], group["var_code"]))
        chunk_maps[(year, module)] = (usecols, rename_map)

    elapsed = time.perf_counter() - t0
    log_ok(f"Built mappings for {len(chunk_maps)} (year, module) chunks")
    log_time(f"Mapping build: {elapsed:.3f}s")

    return chunk_maps


# ============================================================================
# Chunk Processing
# ============================================================================

def process_chunk(
    year: int,
    module: str,
    file_path: Path,
    usecols: List[str],
    rename_map: Dict[str, str]
) -> Optional[pd.DataFrame]:
    """Process a single (year, module) chunk."""
    log_step(f"Processing year={year}, module={module}")
    t0 = time.perf_counter()

    if not file_path.exists():
        log_warn(f"File not found: {file_path}")
        return None

    if not usecols:
        log_warn(f"No columns to read for year={year}, module={module}")
        return None

    log_file(f"Source: {file_path.name}")

    try:
        # Check available columns
        log_read(f"Scanning header: {file_path.name}")
        header = pd.read_csv(file_path, nrows=0)
        available_cols = [col for col in usecols if col in header.columns]

        if not available_cols:
            log_warn(f"None of {len(usecols)} requested columns found in {file_path.name}")
            return None

        missing_cols = len(usecols) - len(available_cols)
        if missing_cols > 0:
            log_info(f"Columns available: {len(available_cols)}/{len(usecols)} (missing: {missing_cols})")

        # Read chunk
        log_read(f"Reading {len(available_cols)} columns from {file_path.name}...")
        df = pd.read_csv(
            file_path,
            usecols=available_cols,
            dtype="string",
            low_memory=False
        )

        if df.empty:
            log_warn(f"Empty dataframe for year={year}, module={module}")
            return None

        log_info(f"Loaded {len(df):,} rows × {len(df.columns)} cols")

        # Rename
        rename_subset = {k: v for k, v in rename_map.items() if k in df.columns}
        if rename_subset:
            df = df.rename(columns=rename_subset)
            log_info(f"Renamed {len(rename_subset)} columns to canonical names")

        # Add metadata
        df.insert(0, "year", year)
        df.insert(1, "module", module)

        # Downcast types
        log_info(f"Downcasting types...")
        df = infer_and_downcast_types(df)

        df["year"] = df["year"].astype("int32")
        df["module"] = df["module"].astype("category")

        elapsed = time.perf_counter() - t0
        log_ok(f"Processed: {len(df):,} rows × {len(df.columns)} cols")
        log_time(f"Chunk processing: {elapsed:.3f}s")

        return df

    except Exception as e:
        log_error(f"Failed to process year={year}, module={module}: {e}")
        return None


def write_chunk_to_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    partition_by_module: bool,
    engine: str
) -> None:
    """Write chunk to partitioned Parquet."""
    if df.empty:
        log_warn("Empty DataFrame, skipping write")
        return

    year = df["year"].iloc[0]
    module = df["module"].iloc[0]

    log_write(f"Writing year={year}, module={module} to Parquet...")
    t0 = time.perf_counter()

    partition_cols = ["year", "module"] if partition_by_module else ["year"]

    try:
        table = pa.Table.from_pandas(df, preserve_index=False)

        pq.write_to_dataset(
            table,
            root_path=str(output_dir),
            partition_cols=partition_cols,
            compression="zstd",
            existing_data_behavior="overwrite_or_ignore"
        )

        elapsed = time.perf_counter() - t0
        log_ok(f"Wrote {len(df):,} rows to partition year={year}, module={module}")
        log_time(f"Parquet write: {elapsed:.3f}s")

    except Exception as e:
        log_error(f"Failed to write Parquet for year={year}, module={module}: {e}")
        raise


# ============================================================================
# Main Pipeline
# ============================================================================

def build_panel(
    data_dir: Path,
    out_dir: Path,
    final_dir: Path,
    partition_by_module: bool,
    engine: str,
    rebuild: bool
) -> int:
    """Main pipeline."""
    try:
        log_title("═" * 70)
        log_title("  PSID Panel Builder — Memory-Efficient Parquet Output")
        log_title("═" * 70)

        pipeline_start = time.perf_counter()

        # Setup
        panel_dir = final_dir / "panel.parquet"

        if rebuild and panel_dir.exists():
            log_info(f"Rebuild requested, removing: {panel_dir}")
            shutil.rmtree(panel_dir)

        panel_dir.mkdir(parents=True, exist_ok=True)
        log_ok(f"Output directory ready: {panel_dir}")

        # Check audit file
        audit_file = out_dir / "codes_resolved_audit.csv"
        if audit_file.exists():
            log_file(f"Found audit file: {audit_file}")

        # Step 1: Load canonical grid
        grid_path = out_dir / "final_grid.csv"
        canonical_vars = load_canonical_grid(grid_path)

        # Step 2: Build chunk mappings
        mapping_path = out_dir / "mapping_long.csv"
        chunk_mappings = build_chunk_mappings(mapping_path, canonical_vars)

        # Step 3: Discover files
        files_map = discover_files(data_dir)

        # Step 4: Process chunks
        log_title("─" * 70)
        log_step("Processing and writing chunks...")
        log_title("─" * 70)

        stats: Dict[int, int] = {}
        years_written: Set[int] = set()

        sorted_chunks = sorted(chunk_mappings.items())

        iterator = tqdm(sorted_chunks, desc="Processing chunks", unit="chunk") if HAS_TQDM else sorted_chunks

        for (year, module), (usecols, rename_map) in iterator:
            file_path = files_map.get((year, module))

            if file_path is None:
                log_warn(f"No file for year={year}, module={module}")
                continue

            df_chunk = process_chunk(year, module, file_path, usecols, rename_map)

            if df_chunk is None or df_chunk.empty:
                continue

            write_chunk_to_parquet(df_chunk, panel_dir, partition_by_module, engine)

            years_written.add(year)
            stats[year] = stats.get(year, 0) + len(df_chunk)

            del df_chunk
            gc.collect()

        # Step 5: Manifest
        pipeline_elapsed = time.perf_counter() - pipeline_start

        log_title("═" * 70)
        log_title("  ✅ PANEL BUILD COMPLETE")
        log_title("═" * 70)

        log_info(f"Output: {panel_dir}")
        log_info(f"Years written: {len(years_written)}")
        log_info(f"Years: {sorted(years_written)}")
        log_info("")
        log_info("Rows per year:")
        for year in sorted(stats.keys()):
            log_file(f"  {year}: {stats[year]:,} rows")

        total_rows = sum(stats.values())
        log_info("")
        log_ok(f"Grand total: {total_rows:,} rows")
        log_time(f"Total pipeline time: {pipeline_elapsed:.3f}s")
        log_title("═" * 70)

        return 0

    except Exception as e:
        log_error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Build memory-efficient partitioned Parquet panel",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--data-dir", type=Path, default=Path("sorted_data"))
    parser.add_argument("--out-dir", type=Path, default=Path("out"))
    parser.add_argument("--final-dir", type=Path, default=Path("final_results"))
    parser.add_argument("--partition-by-module", action="store_true")
    parser.add_argument("--engine", choices=["pyarrow", "fastparquet"], default="pyarrow")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    exit_code = build_panel(
        data_dir=args.data_dir,
        out_dir=args.out_dir,
        final_dir=args.final_dir,
        partition_by_module=args.partition_by_module,
        engine=args.engine,
        rebuild=args.rebuild
    )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
