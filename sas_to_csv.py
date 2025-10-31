#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sas_to_csv.py — Convert SAS/TXT files to CSV with granular progress tracking
==============================================================================
Instrumented for maximum observability with fine-grained progress bars.
"""

import argparse
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None

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
def log_time(msg: str): print(c(f"[TIME] {msg}", 'BLUE'))
def log_error(msg: str): print(c(f"[ERROR] {msg}", 'RED'), file=sys.stderr)


# ============================================================================
# Progress-Aware File Reading
# ============================================================================

def read_fwf_with_progress(txt_path: str, colspecs, names, desc: str):
    """Read fixed-width file with progress bar."""
    if not HAS_TQDM:
        # Fallback without progress
        return pd.read_fwf(txt_path, colspecs=colspecs, names=names, dtype="string")

    # Get file size for progress tracking
    file_size = os.path.getsize(txt_path)

    # Read file in chunks with progress bar
    chunks = []

    with open(txt_path, 'rb') as f:
        with tqdm(total=file_size, desc=desc, unit='B', unit_scale=True,
                  bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

            # Read header to estimate chunk size
            first_line = f.readline()
            line_size = len(first_line)
            f.seek(0)

            # Estimate number of lines
            estimated_lines = file_size // line_size if line_size > 0 else 10000

            # Determine chunk size (aim for ~10 updates)
            chunk_size = max(1000, estimated_lines // 10)

            # Read in chunks using pandas iterator
            reader = pd.read_fwf(
                txt_path,
                colspecs=colspecs,
                names=names,
                dtype="string",
                chunksize=chunk_size
            )

            for chunk in reader:
                chunks.append(chunk)
                # Update progress based on approximate bytes read
                bytes_read = len(chunk) * line_size
                pbar.update(bytes_read)

    # Concatenate all chunks
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def write_csv_with_progress(df, csv_path: str, colnames: list, labels: list, desc: str):
    """Write CSV with progress bar."""
    if not HAS_TQDM:
        # Fallback without progress
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(",".join(colnames) + "\n")
            f.write(",".join(f'"{lab}"' for lab in labels) + "\n")
            df.to_csv(f, index=False, header=False)
        return

    total_rows = len(df) + 2  # +2 for header and label rows

    with open(csv_path, "w", encoding="utf-8") as f:
        with tqdm(total=total_rows, desc=desc, unit='rows',
                  bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

            # Write header
            f.write(",".join(colnames) + "\n")
            pbar.update(1)

            # Write labels
            f.write(",".join(f'"{lab}"' for lab in labels) + "\n")
            pbar.update(1)

            # Write data in chunks
            chunk_size = max(100, len(df) // 20)  # Aim for ~20 updates

            for start_idx in range(0, len(df), chunk_size):
                end_idx = min(start_idx + chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx]
                chunk.to_csv(f, index=False, header=False)
                pbar.update(len(chunk))


# ============================================================================
# Conversion Function
# ============================================================================

def txt_to_csv_full(txt_path: str, layout_path: str, csv_path: str, file_idx: int, total: int) -> None:
    """Convert single SAS/TXT pair to CSV with granular progress."""
    base_name = Path(csv_path).stem.replace("_full", "")

    log_step(f"[{file_idx}/{total}] Converting {base_name}")
    t0 = time.perf_counter()

    log_file(f"Layout: {Path(layout_path).name}")
    log_file(f"Data:   {Path(txt_path).name}")
    log_file(f"Output: {Path(csv_path).name}")

    # Parse layout
    log_info("Parsing layout file...")
    t_parse = time.perf_counter()

    pat_input = re.compile(r'([A-Za-z0-9_]+)\s+(\d+)\s*-\s*(\d+)')
    pat_label = re.compile(r'^\s*([A-Za-z0-9_]+)\s+LABEL="([^"]+)"', re.IGNORECASE)

    raw_cols = []
    labels = {}

    text = Path(layout_path).read_text(encoding="utf-8", errors="ignore").splitlines()

    # Parse with mini progress if large layout
    layout_iter = tqdm(text, desc="  Parsing layout", leave=False, disable=not HAS_TQDM) if len(text) > 5000 else text

    for line in layout_iter:
        for var, a, b in pat_input.findall(line):
            raw_cols.append((var, int(a), int(b)))
        m = pat_label.match(line)
        if m:
            var, lab = m.groups()
            labels[var] = lab

    if not raw_cols:
        raise ValueError(f"No column specs found in layout: {layout_path}")

    raw_cols.sort(key=lambda t: t[1])

    names = [t[0] for t in raw_cols]
    counts = Counter(names)
    seen = Counter()
    colnames = []
    for name in names:
        seen[name] += 1
        colnames.append(f"{name}_{seen[name]}" if counts[name] > 1 else name)

    colspecs = [(start - 1, end) for _, start, end in raw_cols]

    elapsed_parse = time.perf_counter() - t_parse
    log_info(f"Found {len(colnames)} columns, {len(labels)} labels ({elapsed_parse:.2f}s)")

    # Validate line length
    expected_len = max(end for _, _, end in raw_cols)
    file_size_mb = os.path.getsize(txt_path) / (1024 * 1024)
    log_info(f"Expected line length: {expected_len} chars, file size: {file_size_mb:.1f} MB")

    with open(txt_path, 'r', encoding='utf-8', errors='ignore') as ftxt:
        for first in ftxt:
            if first.strip():
                actual_len = len(first.rstrip("\n\r"))
                if actual_len < expected_len:
                    raise ValueError(f"Line too short ({actual_len} < {expected_len})")
                break

    # Read fixed-width file with progress
    log_read(f"Reading fixed-width data from {Path(txt_path).name}...")
    t_read = time.perf_counter()

    df = read_fwf_with_progress(
        txt_path,
        colspecs,
        colnames,
        f"  Reading {base_name}"
    )

    elapsed_read = time.perf_counter() - t_read
    log_info(f"Loaded {len(df):,} rows × {len(df.columns)} columns ({elapsed_read:.2f}s)")

    # Prepare label row
    label_row = [labels.get(name.split("_")[0], "") for name in colnames]

    # Write CSV with progress
    log_write(f"Writing CSV to {Path(csv_path).name}...")
    t_write = time.perf_counter()

    write_csv_with_progress(
        df,
        csv_path,
        colnames,
        label_row,
        f"  Writing {base_name}"
    )

    elapsed_write = time.perf_counter() - t_write

    elapsed = time.perf_counter() - t0
    log_ok(f"Created {Path(csv_path).name}: {len(df):,} rows × {len(df.columns)} cols")
    log_time(f"Total: {elapsed:.2f}s (parse: {elapsed_parse:.2f}s, read: {elapsed_read:.2f}s, write: {elapsed_write:.2f}s)")


# ============================================================================
# Batch Processing
# ============================================================================

FAM_RE  = re.compile(r'^(FAM\d{4}ER)\.(sas|txt)$', re.IGNORECASE)
WLTH_RE = re.compile(r'^(WLTH\d{4})\.(sas|txt)$', re.IGNORECASE)

def parse_file_list(list_path: Path) -> list[str]:
    """Parse file_list.txt."""
    log_read(f"Reading file list: {list_path}")
    lines = [ln.strip() for ln in list_path.read_text(encoding="utf-8").splitlines()]
    filenames = [ln for ln in lines if ln and not ln.startswith("#")]
    log_info(f"Found {len(filenames)} entries in file list")
    return filenames

def group_pairs(filenames: list[str], data_dir: Path):
    """Group .sas and .txt files into pairs."""
    log_info("Grouping SAS/TXT file pairs...")
    pairs = defaultdict(dict)

    for name in filenames:
        m1 = FAM_RE.match(name)
        m2 = WLTH_RE.match(name)
        if m1:
            base, ext = m1.group(1), m1.group(2).lower()
            pairs[base][ext] = data_dir / name
        elif m2:
            base, ext = m2.group(1), m2.group(2).lower()
            pairs[base][ext] = data_dir / name

    log_info(f"Identified {len(pairs)} file pairs")
    return pairs

def run_batch(file_list: Path, out_dir: Path, skip_existing: bool = True):
    """Run batch conversion with nested progress bars."""
    log_title("═" * 70)
    log_title("  SAS/TXT → CSV Batch Conversion")
    log_title("═" * 70)

    pipeline_start = time.perf_counter()

    # Validate inputs
    if not file_list.exists():
        log_error(f"File list not found: {file_list}")
        sys.exit(1)

    if not out_dir.exists():
        log_warn(f"Output directory does not exist, creating: {out_dir}")
        out_dir.mkdir(parents=True, exist_ok=True)

    log_file(f"File list: {file_list}")
    log_file(f"Output directory: {out_dir}")

    # Parse file list
    names = parse_file_list(file_list)
    pairs = group_pairs(names, out_dir)

    if not pairs:
        log_warn("No SAS/TXT pairs detected in file list")
        return

    # Process pairs
    log_title("─" * 70)
    log_step(f"Processing {len(pairs)} file pairs...")
    log_title("─" * 70)

    converted = 0
    skipped = 0
    errors = 0

    sorted_pairs = sorted(pairs.items())

    # Outer progress bar for files
    iterator = tqdm(sorted_pairs, desc="Overall progress", unit="file", position=0, leave=True) if HAS_TQDM else sorted_pairs

    for idx, (base, exts) in enumerate(iterator, 1):
        if 'sas' not in exts or 'txt' not in exts:
            log_warn(f"Incomplete pair for {base} (missing .sas or .txt)")
            errors += 1
            continue

        layout_path = exts['sas']
        txt_path = exts['txt']
        out_path = out_dir / f"{base}_full.csv"

        if skip_existing and out_path.exists():
            log_skip(f"{out_path.name} already exists")
            skipped += 1
            continue

        try:
            txt_to_csv_full(str(txt_path), str(layout_path), str(out_path), idx, len(pairs))
            converted += 1
        except Exception as e:
            log_error(f"Failed to convert {base}: {e}")
            errors += 1

    # Summary
    pipeline_elapsed = time.perf_counter() - pipeline_start

    log_title("═" * 70)
    log_title("  ✅ BATCH CONVERSION COMPLETE")
    log_title("═" * 70)

    log_info(f"Converted: {converted} files")
    log_info(f"Skipped:   {skipped} files (already exist)")
    if errors > 0:
        log_warn(f"Errors:    {errors} files")

    log_time(f"Total time: {pipeline_elapsed:.1f}s ({pipeline_elapsed/60:.1f} min)")
    log_title("═" * 70)


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert SAS/TXT files to CSV format with granular progress",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--file-list",
        type=Path,
        default=Path("file_list.txt"),
        help="Path to file_list.txt (default: file_list.txt)"
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("sorted_data"),
        help="Output directory for CSV files (default: sorted_data)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing CSV files"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    run_batch(
        file_list=args.file_list,
        out_dir=args.out_dir,
        skip_existing=not args.force
    )


if __name__ == "__main__":
    main()
