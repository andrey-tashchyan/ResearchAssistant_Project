#!/usr/bin/env python3
"""Command-line utilities for PSID mapping dictionaries and panel extraction."""
from __future__ import annotations

import argparse
import dataclasses
import itertools
import logging
import math
import multiprocessing as mp
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

EXPECTED_MAPPING_COLUMNS = [
    "canonical",
    "year",
    "file_type",
    "var_code",
    "label",
    "category",
    "dtype",
    "required",
    "transform",
]

ALLOWED_DTYPES = {"int64", "float64", "string", "category"}
DTYPE_ALIAS = {
    "int64": "Int64",
    "float64": "float64",
    "string": "string",
    "category": "category",
}

MODULE_PREFIXES = {"FAM", "WLTH"}
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
TRANSFORM_SPLIT_RE = re.compile(r"\s*;\s*")
NA_CODES_RE = re.compile(r"^na_codes\s*:(.*)$", re.IGNORECASE)
CLIP_RE = re.compile(r"^clip\s*:\s*\[?(?P<min>[^,\]]*),(?P<max>[^\]]*)\]?\s*$", re.IGNORECASE)
WINSOR_RE = re.compile(r"^winsor\s*:\s*\[?(?P<p>[^\]]*)\]?\s*$", re.IGNORECASE)
TRANSFORM_TOKEN_RE = re.compile(r"^([a-zA-Z0-9_]+)(?::(.*))?$")


@dataclass
class TransformOperation:
    """Concrete transform instruction for a canonical column."""

    name: str
    params: Optional[object] = None


@dataclass
class TransformSpec:
    """Parsed transform specification with ordered operations."""

    operations: List[TransformOperation] = field(default_factory=list)
    duplicate_strategy: Optional[str] = None

    def merge(self, other: "TransformSpec") -> None:
        """Merge another spec into this one respecting order and duplicates."""

        if other.duplicate_strategy:
            if self.duplicate_strategy and self.duplicate_strategy != other.duplicate_strategy:
                # keep first strategy, but we can log elsewhere
                return
            if not self.duplicate_strategy:
                self.duplicate_strategy = other.duplicate_strategy
        existing = {(op.name, repr(op.params)) for op in self.operations}
        for op in other.operations:
            key = (op.name, repr(op.params))
            if key not in existing:
                self.operations.append(op)
                existing.add(key)


@dataclass
class CanonicalMeta:
    """Metadata for a canonical variable required for extraction."""

    canonical: str
    dtype: Optional[str]
    transform: TransformSpec
    label: Optional[str]
    category: Optional[str]
    file_type: Optional[str]


@dataclass
class ExtractionOptions:
    """Extraction behaviour flags."""

    apply_transforms: bool
    coerce_types: bool
    sample_rows: Optional[int]
    fail_fast: bool
    quiet: bool


@dataclass
class YearTask:
    """Work unit for a single year."""

    year: int
    files: List[str]
    var_codes_by_canonical: Dict[str, List[str]]
    var_code_to_canonical: Dict[str, str]
    all_canonicals: List[str]
    canonical_meta: Dict[str, CanonicalMeta]
    options: ExtractionOptions


@dataclass
class YearResult:
    """Result returned from processing a single year."""

    year: int
    frame: Optional[pd.DataFrame]
    messages: List[Tuple[str, str]]
    audit_rows: List[Dict[str, object]]


def configure_logging(quiet: bool) -> logging.Logger:
    """Configure root logger once and return module logger."""

    logger = logging.getLogger("psid_tool")
    if logger.handlers:
        logger.setLevel(logging.WARNING if quiet else logging.INFO)
        return logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.WARNING if quiet else logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
    logger.setLevel(logging.WARNING if quiet else logging.INFO)
    return logger


def normalize_header(name: str) -> str:
    """Normalize column name for consistent matching."""

    return unicodedata.normalize("NFC", str(name)).strip()


def parse_year_filters(arg: Optional[str], logger: logging.Logger, fail_fast: bool) -> Optional[Set[int]]:
    """Parse comma-separated years and ranges into a set of ints."""

    if not arg:
        return None
    years: Set[int] = set()
    for part in arg.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            try:
                start = int(start_str)
                end = int(end_str)
            except ValueError as err:
                msg = f"Invalid year range '{part}': {err}"
                if fail_fast:
                    raise ValueError(msg) from err
                logger.warning(msg)
                continue
            if start > end:
                start, end = end, start
            for year in range(start, end + 1):
                years.add(year)
        else:
            try:
                years.add(int(part))
            except ValueError as err:
                msg = f"Invalid year '{part}': {err}"
                if fail_fast:
                    raise ValueError(msg) from err
                logger.warning(msg)
    return years


def parse_modules(arg: Optional[str]) -> Set[str]:
    """Parse modules flag into normalized module names."""

    if not arg:
        return set(MODULE_PREFIXES)
    modules = {item.strip().upper() for item in arg.split(",") if item.strip()}
    return {m for m in modules if m in MODULE_PREFIXES}


def read_mapping(path: Path, logger: logging.Logger, fail_fast: bool) -> pd.DataFrame:
    """Load mapping_long.csv and validate required columns."""

    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")
    df = pd.read_csv(path)
    missing = [col for col in EXPECTED_MAPPING_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Mapping file missing columns: {', '.join(missing)}")
    df = df[EXPECTED_MAPPING_COLUMNS].copy()
    df["canonical"] = df["canonical"].astype(str).str.strip()
    df["var_code"] = df["var_code"].apply(normalize_header)
    df["file_type"] = df["file_type"].astype(str).str.upper()
    df["label"] = df["label"].astype(str).replace({"nan": ""})
    df["category"] = df["category"].astype(str).replace({"nan": ""})
    df["dtype"] = df["dtype"].astype(str).str.lower().replace({"nan": ""})
    df["transform"] = df["transform"].astype(str).replace({"nan": ""})
    years = []
    invalid_years = 0
    for raw in df["year"]:
        try:
            year = int(raw)
        except (ValueError, TypeError):
            invalid_years += 1
            year = None
        if year and YEAR_PATTERN.match(str(year)):
            years.append(year)
        else:
            if year is None:
                pass
            else:
                invalid_years += 1
            years.append(pd.NA)
    df["year"] = pd.Series(years, dtype="Int64")
    if invalid_years:
        msg = f"Dropped {invalid_years} rows with invalid year values"
        if fail_fast:
            raise ValueError(msg)
        logger.warning(msg)
        df = df.dropna(subset=["year"])  # type: ignore[arg-type]
    df["required"] = pd.to_numeric(df["required"], errors="coerce").fillna(0).astype(int)
    return df


def filter_mapping(
    df: pd.DataFrame,
    years: Optional[Set[int]],
    modules: Set[str],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Filter mapping rows by year and module."""

    out = df
    if years is not None:
        out = out[out["year"].isin(years)]
    if modules and modules != MODULE_PREFIXES:
        out = out[out["file_type"].isin(modules)]
    out = out.reset_index(drop=True)
    if out.empty:
        raise ValueError("Mapping filter produced zero rows; check --years/--modules filters")
    return out


def build_dictionary_outputs(
    df: pd.DataFrame,
    out_dir: Path,
    logger: logging.Logger,
    wide_only: bool = False,
    long_only: bool = False,
) -> None:
    """Construct dictionary CSVs grouped by inferred labels."""

    if wide_only and long_only:
        raise ValueError("Cannot request both --wide-only and --long-only")

    if "label_en" not in df.columns:
        raise ValueError("label_en column is required to build dictionaries")

    df_sorted = df.sort_values(["label_en", "year", "var_code"]).reset_index(drop=True)
    if df_sorted.empty:
        raise ValueError("No mapping rows available to build dictionary")

    def first_non_empty(series: pd.Series) -> str:
        for value in series:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def sorted_unique_strings(series: pd.Series) -> List[str]:
        return sorted({val for val in series if isinstance(val, str) and val})

    def first_string(series: pd.Series) -> str:
        vals = sorted_unique_strings(series)
        return vals[0] if vals else ""

    label_meta = (
        df_sorted.groupby("label_en", dropna=False)
        .agg(
            canonical_list=("canonical", lambda s: " | ".join(sorted_unique_strings(s)) or ""),
            category=("category", first_non_empty),
            dtype=("dtype", first_non_empty),
            required_any=("required", lambda s: int(s.max() if len(s) else 0)),
            example_var_code=("var_code", first_string),
            modules=("file_type", lambda s: tuple(sorted_unique_strings(s))),
        )
        .reset_index()
    )

    label_meta["module_any"] = label_meta["modules"].apply(
        lambda mods: (mods[0] if len(mods) == 1 else "Multiple") if mods else ""
    )
    label_meta.drop(columns="modules", inplace=True)
    label_meta["tokens"] = label_meta["label_en"].apply(lambda v: " ".join(str(v).split()))
    label_meta["years_present"] = (
        df_sorted.groupby("label_en")
        ["year"]
        .apply(lambda years: "; ".join(str(int(y)) for y in sorted({int(y) for y in years.dropna()})))
        .reindex(label_meta["label_en"])
        .fillna("")
        .tolist()
    )

    label_year_codes = (
        df_sorted.groupby(["label_en", "year"], dropna=False)["var_code"]
        .agg(lambda s: " | ".join(sorted({normalize_header(v) for v in s if v})))
        .reset_index()
    )
    years_sorted = sorted(df_sorted["year"].dropna().unique().tolist())

    if not long_only:
        wide = label_meta.copy()
        for year in years_sorted:
            subset = label_year_codes[label_year_codes["year"] == year]
            mapping = dict(zip(subset["label_en"], subset["var_code"]))
            wide[str(int(year))] = wide["label_en"].map(mapping).fillna("")
        ordered_cols = [
            "label_en",
            "module_any",
            "example_var_code",
            "canonical_list",
            "tokens",
            "category",
            "dtype",
            "required_any",
            "years_present",
        ] + [str(int(year)) for year in years_sorted]
        wide = wide[ordered_cols]
        out_path = out_dir / "mapping_dictionary_wide.csv"
        wide.to_csv(out_path, index=False)
        logger.info("Wrote %s (%d rows)", out_path.name, len(wide))

    if not wide_only:
        codes_records: List[Dict[str, str]] = []
        for label, group in label_year_codes.groupby("label_en", dropna=False):
            pieces = []
            for _, row in group.sort_values("year").iterrows():
                code_str = row["var_code"] or ""
                pieces.append(f"{int(row['year'])}: {code_str}" if code_str else f"{int(row['year'])}:")
            codes_records.append({"label_en": label, "codes_by_year": "; ".join(pieces)})
        codes_by_label = (
            pd.DataFrame(codes_records)
            if codes_records
            else pd.DataFrame(columns=["label_en", "codes_by_year"])
        )
        long_df = label_meta.merge(codes_by_label, on="label_en", how="left")
        ordered_cols = [
            "label_en",
            "module_any",
            "example_var_code",
            "canonical_list",
            "tokens",
            "category",
            "dtype",
            "required_any",
            "codes_by_year",
        ]
        long_df = long_df[ordered_cols].sort_values("label_en").reset_index(drop=True)
        out_path = out_dir / "mapping_dictionary_long.csv"
        long_df.to_csv(out_path, index=False)
        logger.info("Wrote %s (%d rows)", out_path.name, len(long_df))


def gather_files(data_dir: Path, modules: Set[str], years: Optional[Set[int]]) -> Dict[int, List[Path]]:
    """Map year to CSV files filtered by module prefixes."""

    files_by_year: Dict[int, List[Path]] = defaultdict(list)
    for path in sorted(data_dir.glob("*.csv")):
        if not path.name.lower().endswith(".csv"):
            continue
        name_upper = path.name.upper()
        module = next((m for m in MODULE_PREFIXES if name_upper.startswith(m)), None)
        if not module or module not in modules:
            continue
        match = YEAR_PATTERN.search(path.name)
        if not match:
            continue
        year = int(match.group(0))
        if years is not None and year not in years:
            continue
        files_by_year[year].append(path)
    for year in files_by_year:
        files_by_year[year] = sorted(files_by_year[year], key=lambda p: p.name)
    return dict(sorted(files_by_year.items()))


def augment_with_labels(
    df: pd.DataFrame,
    data_dir: Path,
    modules: Set[str],
    logger: logging.Logger,
    fail_fast: bool,
) -> pd.DataFrame:
    """Attach label_en values from the first data row of module CSV files."""

    target_years = sorted({int(year) for year in df["year"].dropna().unique().tolist()})
    if not target_years:
        df["label_en"] = df.get("label", "")
        return df

    files_by_year = gather_files(data_dir, modules, set(target_years))
    if not files_by_year:
        msg = "No matching CSV files found for label enrichment"
        if fail_fast:
            raise FileNotFoundError(msg)
        logger.warning(msg)
        df["label_en"] = df.get("label", "")
        return df

    label_lookup: Dict[Tuple[str, int, str], str] = {}
    for year, paths in files_by_year.items():
        for path in paths:
            module = next((m for m in MODULE_PREFIXES if path.name.upper().startswith(m)), None)
            if module is None:
                continue
            try:
                header_df = pd.read_csv(
                    path,
                    nrows=1,
                    dtype=str,
                    na_filter=False,
                    low_memory=False,
                )
            except Exception as err:
                msg = f"Failed to read labels from {path.name}: {err}"
                if fail_fast:
                    raise
                logger.warning(msg)
                continue
            if header_df.empty:
                logger.warning("File %s has no rows to infer labels", path.name)
                continue
            header_df.columns = [normalize_header(col) for col in header_df.columns]
            row = header_df.iloc[0]
            for col in header_df.columns:
                key = (module, int(year), col)
                label_raw = str(row[col]).strip()
                if key not in label_lookup or (not label_lookup[key] and label_raw):
                    label_lookup[key] = label_raw
            logger.info("Inferred labels from %s (%d columns)", path.name, header_df.shape[1])

    labels_df = pd.DataFrame(
        [
            {"file_type": ft, "year": year, "var_code": var, "label_en": label_lookup[(ft, year, var)]}
            for (ft, year, var) in sorted(label_lookup.keys())
        ]
    )
    if labels_df.empty:
        logger.warning("No labels inferred from data files; fallback to existing labels")
        df["label_en"] = df.get("label", "")
        return df

    labels_df["year"] = labels_df["year"].astype("Int64")
    df = df.merge(labels_df, on=["file_type", "year", "var_code"], how="left")
    df["label_en"] = df["label_en"].fillna("").astype(str).str.strip()
    if "label" in df.columns:
        fallback = df["label"].astype(str).str.strip()
    else:
        fallback = pd.Series(["" for _ in range(len(df))], index=df.index)
    df["label_en"] = df["label_en"].mask(df["label_en"].isin(["", "nan", "None"]), fallback)
    mask_unknown = df["label_en"].isin(["", "nan", "None"])
    df.loc[mask_unknown, "label_en"] = "Unknown " + df.loc[mask_unknown, "var_code"].astype(str)
    missing_count = int(mask_unknown.sum())
    if missing_count:
        logger.warning("Applied Unknown fallback labels for %d variables", missing_count)
    if "label" in df.columns:
        df["label"] = df["label"].astype(str)
        empty_mask = df["label"].isin(["", "nan", "None"])
        df.loc[empty_mask, "label"] = df.loc[empty_mask, "label_en"]
    else:
        df["label"] = df["label_en"]
    return df


def parse_transform_string(
    value: str,
    canonical: str,
    logger: logging.Logger,
    fail_fast: bool,
) -> TransformSpec:
    """Parse a transform cell into a specification."""

    spec = TransformSpec()
    if not value or value.strip().lower() in {"nan", "none", ""}:
        return spec
    tokens = TRANSFORM_SPLIT_RE.split(value.strip()) if ";" in value else [value.strip()]
    for raw in tokens:
        if not raw:
            continue
        token = raw.strip()
        lower = token.lower()
        if lower in {"first_non_na", "sum_non_na", "max_non_na"}:
            if spec.duplicate_strategy and spec.duplicate_strategy != lower:
                msg = (
                    f"Conflicting duplicate strategies for {canonical}: {spec.duplicate_strategy} vs {lower}"
                )
                if fail_fast:
                    raise ValueError(msg)
                logger.warning(msg)
            else:
                spec.duplicate_strategy = lower
            continue
        if lower == "to_int":
            spec.operations.append(TransformOperation("to_int"))
            continue
        if lower == "to_float":
            spec.operations.append(TransformOperation("to_float"))
            continue
        if lower == "strip":
            spec.operations.append(TransformOperation("strip"))
            continue
        if lower == "lower":
            spec.operations.append(TransformOperation("lower"))
            continue
        if lower == "upper":
            spec.operations.append(TransformOperation("upper"))
            continue
        if lower == "na_blank_to_nan":
            spec.operations.append(TransformOperation("na_blank_to_nan"))
            continue
        na_match = NA_CODES_RE.match(token)
        if na_match:
            payload = na_match.group(1).strip()
            parts = [part.strip() for part in payload.strip("[]").split("|") if part.strip()]
            cleaned: List[object] = []
            for part in parts:
                if part.lower() == "nan":
                    continue
                try:
                    if "." in part or "e" in part.lower():
                        cleaned.append(float(part))
                    else:
                        cleaned.append(int(part))
                except ValueError:
                    cleaned.append(part)
            spec.operations.append(TransformOperation("na_codes", cleaned))
            continue
        clip_match = CLIP_RE.match(token)
        if clip_match:
            min_str = clip_match.group("min").strip()
            max_str = clip_match.group("max").strip()
            min_val = float(min_str) if min_str else None
            max_val = float(max_str) if max_str else None
            spec.operations.append(TransformOperation("clip", (min_val, max_val)))
            continue
        winsor_match = WINSOR_RE.match(token)
        if winsor_match:
            p_str = winsor_match.group("p").strip()
            try:
                p_val = float(p_str)
            except ValueError as err:
                msg = f"Invalid winsor parameter for {canonical}: {token}"
                if fail_fast:
                    raise ValueError(msg) from err
                logger.warning(msg)
                continue
            if not 0 < p_val < 0.5:
                msg = f"Winsor parameter must be between 0 and 0.5 (exclusive) for {canonical}: {p_val}"
                if fail_fast:
                    raise ValueError(msg)
                logger.warning(msg)
                continue
            spec.operations.append(TransformOperation("winsor", p_val))
            continue
        match = TRANSFORM_TOKEN_RE.match(token)
        if match:
            name = match.group(1).lower()
            msg = f"Unknown transform token '{token}' for {canonical}"
            if fail_fast:
                raise ValueError(msg)
            logger.warning(msg)
            continue
        msg = f"Unparsable transform token '{token}' for {canonical}"
        if fail_fast:
            raise ValueError(msg)
        logger.warning(msg)
    return spec


def canonical_metadata(
    df: pd.DataFrame,
    logger: logging.Logger,
    fail_fast: bool,
) -> Dict[str, CanonicalMeta]:
    """Aggregate canonical-level metadata from mapping rows."""

    meta: Dict[str, CanonicalMeta] = {}
    for canonical, sub in df.groupby("canonical"):
        dtype_values = [dtype for dtype in sub["dtype"].tolist() if dtype and dtype.lower() != "nan"]
        dtype_choice: Optional[str] = None
        if dtype_values:
            unique = []
            seen = set()
            for val in dtype_values:
                if val not in seen:
                    unique.append(val)
                    seen.add(val)
            dtype_choice = unique[0]
            if len(unique) > 1:
                msg = f"Canonical {canonical} has conflicting dtype values {unique}; using {dtype_choice}"
                if fail_fast:
                    raise ValueError(msg)
                logger.warning(msg)
            if dtype_choice not in ALLOWED_DTYPES:
                msg = f"Canonical {canonical} has unsupported dtype '{dtype_choice}'"
                if fail_fast:
                    raise ValueError(msg)
                logger.warning(msg)
                dtype_choice = None
        transforms = TransformSpec()
        for raw in sub["transform"].tolist():
            parsed = parse_transform_string(raw, canonical, logger, fail_fast)
            transforms.merge(parsed)
        label = next(
            (
                lbl
                for lbl in sub.get("label_en", pd.Series(dtype=str)).tolist()
                if isinstance(lbl, str) and lbl and lbl.lower() != "nan"
            ),
            None,
        )
        if not label:
            label = next(
                (
                    lbl
                    for lbl in sub["label"].tolist()
                    if isinstance(lbl, str) and lbl and lbl.lower() != "nan"
                ),
                "",
            )
        category = next((cat for cat in sub["category"] if cat and cat.lower() != "nan"), "")
        file_type = next((ft for ft in sub["file_type"] if ft), "")
        meta[canonical] = CanonicalMeta(
            canonical=canonical,
            dtype=dtype_choice,
            transform=transforms,
            label=label or None,
            category=category or None,
            file_type=file_type or None,
        )
    return meta


def mapping_for_year(df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """Split mapping by year for quicker access."""

    return {int(year): sub.reset_index(drop=True) for year, sub in df.groupby("year")}


def first(iterable: Iterable[pd.Series]) -> pd.Series:
    """Return the first element from an iterable of Series."""

    for item in iterable:
        return item
    raise ValueError("Iterable is empty")


def resolve_duplicates(
    canonical: str,
    block: pd.DataFrame,
    meta: CanonicalMeta,
    options: ExtractionOptions,
    messages: List[Tuple[str, str]],
) -> Tuple[pd.Series, int, int]:
    """Collapse duplicate canonical columns using configured strategy."""

    resolved = 0
    unresolved = 0
    if block.shape[1] == 0:
        return pd.Series(pd.NA, index=block.index, name=canonical), resolved, unresolved
    if block.shape[1] == 1:
        series = block.iloc[:, 0].copy()
        series.name = canonical
        return series, resolved, unresolved
    strategy = meta.transform.duplicate_strategy if meta else None
    if strategy == "first_non_na":
        series = block.ffill(axis=1).bfill(axis=1).iloc[:, 0]
        resolved = block.shape[1] - 1
        return series.rename(canonical), resolved, unresolved
    if strategy == "sum_non_na":
        numeric = block.apply(pd.to_numeric, errors="coerce")
        series = numeric.sum(axis=1, min_count=1)
        resolved = block.shape[1] - 1
        return series.rename(canonical), resolved, unresolved
    if strategy == "max_non_na":
        numeric = block.apply(pd.to_numeric, errors="coerce")
        series = numeric.max(axis=1)
        resolved = block.shape[1] - 1
        return series.rename(canonical), resolved, unresolved
    if options.apply_transforms:
        msg = (
            f"Canonical {canonical} has multiple source columns but no duplicate strategy defined"
        )
        if options.fail_fast:
            raise ValueError(msg)
        messages.append(("ERROR", msg))
    else:
        msg = (
            f"Canonical {canonical} duplicates encountered; keeping first column because transforms are off"
        )
        messages.append(("WARNING", msg))
    series = block.iloc[:, 0].copy().rename(canonical)
    unresolved = block.shape[1] - 1
    return series, resolved, unresolved


def apply_dtype(series: pd.Series, meta: CanonicalMeta, options: ExtractionOptions, messages: List[Tuple[str, str]]) -> pd.Series:
    """Coerce dtype according to mapping metadata."""

    if not options.coerce_types or not meta.dtype:
        return series
    dtype_key = meta.dtype.lower()
    if dtype_key not in ALLOWED_DTYPES:
        messages.append(("WARNING", f"Skipping unsupported dtype '{meta.dtype}' for {meta.canonical}"))
        return series
    try:
        if dtype_key == "int64":
            coerced = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif dtype_key == "float64":
            coerced = pd.to_numeric(series, errors="coerce").astype("float64")
        elif dtype_key == "string":
            coerced = series.astype("string")
        elif dtype_key == "category":
            coerced = series.astype("category")
        else:
            coerced = series
        coerced.name = series.name
        return coerced
    except Exception as err:  # pragma: no cover - defensive
        msg = f"Failed to coerce {series.name} to {dtype_key}: {err}"
        if options.fail_fast:
            raise
        messages.append(("WARNING", msg))
        return series


def apply_transforms(
    series: pd.Series,
    meta: CanonicalMeta,
    options: ExtractionOptions,
    messages: List[Tuple[str, str]],
) -> pd.Series:
    """Apply ordered transform operations to a column."""

    if not options.apply_transforms or not meta.transform.operations:
        return series
    result = series
    for op in meta.transform.operations:
        name = op.name
        if name == "na_codes":
            codes = op.params or []
            temp = result
            for code in codes:
                temp = temp.mask(temp == code)
                temp = temp.mask(temp.astype("string") == str(code))
            result = temp
            continue
        if name == "strip":
            result = result.astype("string").str.strip()
            continue
        if name == "lower":
            result = result.astype("string").str.lower()
            continue
        if name == "upper":
            result = result.astype("string").str.upper()
            continue
        if name == "na_blank_to_nan":
            result = result.mask(result == "").mask(result.astype("string") == "")
            continue
        if name == "clip":
            min_val, max_val = op.params  # type: ignore[misc]
            numeric = pd.to_numeric(result, errors="coerce")
            result = numeric.clip(lower=min_val, upper=max_val)
            continue
        if name == "winsor":
            p_val = op.params  # type: ignore[assignment]
            numeric = pd.to_numeric(result, errors="coerce")
            lower = numeric.quantile(p_val)
            upper = numeric.quantile(1 - p_val)
            result = numeric.clip(lower=lower, upper=upper)
            continue
        if name == "to_int":
            result = pd.to_numeric(result, errors="coerce").astype("Int64")
            continue
        if name == "to_float":
            result = pd.to_numeric(result, errors="coerce").astype("float64")
            continue
        # Unknown ops already filtered during parsing; guard defensively
        messages.append(("WARNING", f"Skipping unsupported operation '{name}' for {meta.canonical}"))
    result.name = series.name
    return result


def process_year(task: YearTask) -> YearResult:
    """Worker-friendly wrapper to process a single year."""

    messages: List[Tuple[str, str]] = []
    audit_rows: List[Dict[str, object]] = []
    required_canonicals = set(task.all_canonicals)
    if not task.files:
        messages.append(("WARNING", f"No files found for year {task.year}"))
        return YearResult(task.year, None, messages, audit_rows)

    combined_frames: List[pd.DataFrame] = []
    assigned_var_codes: Dict[str, Path] = {}
    for file_str in task.files:
        path = Path(file_str)
        try:
            header_df = pd.read_csv(path, nrows=0)
        except Exception as err:
            msg = f"Failed to read header for {path.name}: {err}"
            if task.options.fail_fast:
                raise
            messages.append(("WARNING", msg))
            continue
        header_cols = [normalize_header(col) for col in header_df.columns]
        header_set = set(header_cols)
        needed_codes = set(task.var_code_to_canonical.keys()) - set(assigned_var_codes.keys())
        available = sorted(code for code in needed_codes if code in header_set)
        duplicated = sorted(code for code in header_set if code in assigned_var_codes)
        if duplicated:
            messages.append(
                (
                    "WARNING",
                    f"Year {task.year} file {path.name} contains already assigned columns: {', '.join(duplicated)}",
                )
            )
        if not available:
            audit_rows.append(
                {
                    "year": task.year,
                    "file": path.name,
                    "present_cols": 0,
                    "missing_required": len(required_canonicals),
                    "duplicate_canonicals_resolved": 0,
                    "duplicate_canonicals_unresolved": 0,
                }
            )
            continue
        for code in available:
            assigned_var_codes[code] = path
        read_kwargs = {
            "usecols": available,
            "low_memory": False,
            "skiprows": [1],
        }
        if task.options.sample_rows:
            read_kwargs["nrows"] = task.options.sample_rows
        try:
            read_kwargs["dtype_backend"] = "numpy_nullable"
            frame = pd.read_csv(path, **read_kwargs)
        except TypeError:
            read_kwargs.pop("dtype_backend", None)
            frame = pd.read_csv(path, **read_kwargs)
        except Exception as err:
            msg = f"Failed to read {path.name}: {err}"
            if task.options.fail_fast:
                raise
            messages.append(("WARNING", msg))
            continue
        if frame.empty:
            retry_kwargs = {k: v for k, v in read_kwargs.items() if k != "skiprows"}
            try:
                frame = pd.read_csv(path, **retry_kwargs)
            except Exception as err:  # pragma: no cover - defensive
                msg = f"Failed to re-read {path.name} without skipping label row: {err}"
                if task.options.fail_fast:
                    raise
                messages.append(("WARNING", msg))
                continue
        frame.columns = [normalize_header(col) for col in frame.columns]
        combined_frames.append(frame)
        audit_rows.append(
            {
                "year": task.year,
                "file": path.name,
                "present_cols": len(available),
                "missing_required": len(required_canonicals) - len({task.var_code_to_canonical[c] for c in assigned_var_codes}),
                "duplicate_canonicals_resolved": 0,
                "duplicate_canonicals_unresolved": 0,
            }
        )
    if not combined_frames:
        messages.append(("ERROR", f"No usable files for year {task.year}"))
        return YearResult(task.year, None, messages, audit_rows)
    combined = pd.concat(combined_frames, axis=1, copy=False)
    combined.columns = [normalize_header(col) for col in combined.columns]
    year_index = combined.index

    canonical_columns: Dict[str, pd.Series] = {}
    duplicate_resolved = Counter()
    duplicate_unresolved = Counter()
    for canonical in sorted(task.all_canonicals):
        codes = task.var_codes_by_canonical.get(canonical, [])
        available_codes = [code for code in codes if code in combined.columns]
        if not available_codes:
            meta = task.canonical_meta.get(canonical)
            series = pd.Series(pd.NA, index=year_index, name=canonical)
            series = apply_dtype(series, meta, task.options, messages)
            series = apply_transforms(series, meta, task.options, messages)
            canonical_columns[canonical] = series
            continue
        block = combined[available_codes]
        meta = task.canonical_meta.get(canonical)
        series, resolved, unresolved = resolve_duplicates(canonical, block, meta, task.options, messages)
        duplicate_resolved[canonical] += resolved
        duplicate_unresolved[canonical] += unresolved
        series = apply_dtype(series, meta, task.options, messages)
        series = apply_transforms(series, meta, task.options, messages)
        canonical_columns[canonical] = series

    ordered_canonicals = sorted(canonical_columns.keys())
    year_frame = pd.DataFrame({col: canonical_columns[col] for col in ordered_canonicals})
    year_frame.insert(0, "year", task.year)
    # Update audit entries with duplicate stats
    for row in audit_rows:
        row["duplicate_canonicals_resolved"] = sum(duplicate_resolved.values())
        row["duplicate_canonicals_unresolved"] = sum(duplicate_unresolved.values())
        present_canonicals = {task.var_code_to_canonical.get(code) for code in assigned_var_codes}
        present_canonicals.discard(None)
        row["missing_required"] = max(0, len(required_canonicals - set(present_canonicals)))
    messages.append(("INFO", f"Processed year {task.year} with {len(year_frame)} rows"))
    return YearResult(task.year, year_frame, messages, audit_rows)


def run_extract(
    df_required: pd.DataFrame,
    files_by_year: Dict[int, List[Path]],
    meta: Dict[str, CanonicalMeta],
    options: ExtractionOptions,
    out_dir: Path,
    index_cols: Optional[List[str]],
    logger: logging.Logger,
    parallel: int,
) -> None:
    """Run panel extraction for required canonicals."""

    if df_required.empty:
        raise ValueError("No required==1 rows found after filtering")
    var_codes_by_canonical: Dict[int, Dict[str, List[str]]] = {}
    var_code_to_canonical_by_year: Dict[int, Dict[str, str]] = {}
    for year, sub in df_required.groupby("year"):
        canonical_map: Dict[str, List[str]] = defaultdict(list)
        reverse_map: Dict[str, str] = {}
        for _, row in sub.iterrows():
            canonical_map[row["canonical"]].append(row["var_code"])
            if row["var_code"] in reverse_map and reverse_map[row["var_code"]] != row["canonical"]:
                msg = (
                    f"Var code {row['var_code']} maps to multiple canonicals ({reverse_map[row['var_code']]}, {row['canonical']})"
                )
                if options.fail_fast:
                    raise ValueError(msg)
                logger.warning(msg)
            reverse_map[row["var_code"]] = row["canonical"]
        var_codes_by_canonical[int(year)] = canonical_map
        var_code_to_canonical_by_year[int(year)] = reverse_map

    tasks: List[YearTask] = []
    all_canonicals = sorted(meta.keys())
    for year, files in files_by_year.items():
        if year not in var_codes_by_canonical:
            continue
        task = YearTask(
            year=year,
            files=[str(path) for path in files],
            var_codes_by_canonical=var_codes_by_canonical[year],
            var_code_to_canonical=var_code_to_canonical_by_year[year],
            all_canonicals=all_canonicals,
            canonical_meta=meta,
            options=options,
        )
        tasks.append(task)

    if not tasks:
        raise ValueError("No overlapping years between mapping and data files")

    results: List[YearResult] = []
    if parallel and parallel > 1:
        ctx = mp.get_context("spawn") if sys.platform == "win32" else mp.get_context()
        with ctx.Pool(processes=parallel) as pool:
            for result in pool.map(process_year, tasks):
                results.append(result)
    else:
        for task in tasks:
            results.append(process_year(task))

    frames: List[pd.DataFrame] = []
    audit_rows: List[Dict[str, object]] = []
    canonical_inventory: Set[str] = set()
    for result in sorted(results, key=lambda r: r.year):
        for level, message in result.messages:
            if level == "INFO":
                logger.info(message)
            elif level == "WARNING":
                logger.warning(message)
            else:
                logger.error(message)
        audit_rows.extend(result.audit_rows)
        if result.frame is None:
            continue
        if "year" in result.frame.columns:
            canonical_inventory.update([col for col in result.frame.columns if col != "year"])
        else:
            canonical_inventory.update(result.frame.columns)
        frames.append(result.frame)

    if not frames:
        raise ValueError("No year produced data; aborting extraction")

    canonical_list = sorted(canonical_inventory)
    aligned_frames = []
    for frame in frames:
        frame = frame.copy()
        for canonical in canonical_list:
            if canonical not in frame.columns:
                frame[canonical] = pd.NA
        ordered_cols = ["year"] + canonical_list
        aligned_frames.append(frame[ordered_cols])

    panel = pd.concat(aligned_frames, axis=0, ignore_index=True)
    panel.sort_values("year", inplace=True)
    if index_cols:
        existing = [col for col in index_cols if col in panel.columns]
        panel.set_index(existing, inplace=True)
        panel.sort_index(inplace=True)
    panel_path = out_dir / "panel_required_only.csv"
    panel.to_csv(panel_path, index=bool(index_cols))
    logger.info("Wrote %s (%d rows, %d columns)", panel_path.name, panel.shape[0], panel.shape[1])
    audit_path = out_dir / "panel_extract_log.csv"
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(audit_path, index=False)
    logger.info("Wrote %s (%d rows)", audit_path.name, len(audit_df))
    logger.info("Canonicals included: %s", ", ".join(canonical_list))
    years_processed = sorted({frame["year"].iloc[0] for frame in aligned_frames})
    logger.info("Years processed: %s", ", ".join(str(year) for year in years_processed))


def run_dict(
    mapping_path: Path,
    data_dir: Path,
    out_dir: Path,
    years: Optional[Set[int]],
    modules: Set[str],
    logger: logging.Logger,
    wide_only: bool,
    long_only: bool,
    fail_fast: bool,
) -> None:
    """Load mapping and produce dictionary outputs."""

    df = read_mapping(mapping_path, logger, fail_fast)
    df = filter_mapping(df, years, modules, logger)
    df = augment_with_labels(df, data_dir, modules, logger, fail_fast)
    out_dir.mkdir(parents=True, exist_ok=True)
    build_dictionary_outputs(df, out_dir, logger, wide_only=wide_only, long_only=long_only)


def run_extract_cli(
    mapping_path: Path,
    data_dir: Path,
    out_dir: Path,
    years: Optional[Set[int]],
    modules: Set[str],
    logger: logging.Logger,
    options: ExtractionOptions,
    index_cols: Optional[List[str]],
    parallel: int,
    fail_fast: bool,
) -> None:
    """Entry point for extract subcommand."""

    df = read_mapping(mapping_path, logger, fail_fast)
    df = filter_mapping(df, years, modules, logger)
    df = augment_with_labels(df, data_dir, modules, logger, fail_fast)
    df_required = df[df["required"] == 1].copy()
    if df_required.empty:
        raise ValueError("No rows with required==1 after filtering")
    meta = canonical_metadata(df_required, logger, fail_fast)
    files_by_year = gather_files(data_dir, modules, years)
    if not files_by_year:
        raise ValueError("No matching CSV files found in data directory")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_extract(df_required, files_by_year, meta, options, out_dir, index_cols, logger, parallel)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Build the top-level argument parser and parse CLI args."""

    parser = argparse.ArgumentParser(description="PSID mapping utilities")
    parser.add_argument("command", choices=["dict", "extract", "all"], help="Action to perform")
    parser.add_argument("--mapping", type=Path, default=Path("mapping_long.csv"), help="Path to mapping_long.csv")
    parser.add_argument("--data-dir", type=Path, default=Path("."), help="Directory containing PSID CSV files")
    parser.add_argument("--out-dir", type=Path, default=Path("."), help="Output directory")
    parser.add_argument("--years", type=str, default=None, help="Comma-separated list/ranges of years")
    parser.add_argument("--modules", type=str, default="FAM,WLTH", help="Comma-separated modules to include")
    parser.add_argument("--fail-fast", action="store_true", help="Abort on first critical error")
    parser.add_argument("--quiet", action="store_true", help="Reduce logging verbosity")
    parser.add_argument("--parallel", type=int, default=1, help="Parallel processes for extraction")
    parser.add_argument("--sample-rows", type=int, default=None, help="Limit rows read per file (extract)")
    parser.add_argument("--wide-only", action="store_true", help="Only write wide dictionary (dict)")
    parser.add_argument("--long-only", action="store_true", help="Only write long dictionary (dict)")
    parser.add_argument("--coerce-types", action="store_true", help="Coerce dtypes during extraction")
    parser.add_argument("--apply-transforms", action="store_true", help="Apply transform DSL during extraction")
    parser.add_argument("--index-cols", type=str, default=None, help="Comma-separated columns to index in panel")
    return parser.parse_args(argv)


def run_selftest() -> None:
    """Embedded smoke test covering dict and extract pipelines."""

    import tempfile

    logger = configure_logging(True)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        mapping = pd.DataFrame(
            {
                "canonical": ["income", "income", "wealth"],
                "year": [1999, 2001, 1999],
                "file_type": ["FAM", "FAM", "WLTH"],
                "var_code": ["ER1", "ER1", "S1"],
                "label": ["Income", "Income", "Wealth"],
                "category": ["Demographics", "Demographics", "Assets"],
                "dtype": ["float64", "float64", "float64"],
                "required": [1, 1, 1],
                "transform": ["", "", ""],
            }
        )
        mapping_path = tmp_path / "mapping_long.csv"
        mapping.to_csv(mapping_path, index=False)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        pd.DataFrame({"ER1": ["Total Income", "1.0", "2.0"]}).to_csv(
            data_dir / "FAM1999_full.csv", index=False
        )
        pd.DataFrame({"ER1": ["Total Income", "3.0"]}).to_csv(
            data_dir / "FAM2001_full.csv", index=False
        )
        pd.DataFrame({"S1": ["Net Worth", "10.0", "20.0"]}).to_csv(
            data_dir / "WLTH1999_full.csv", index=False
        )
        out_dir = tmp_path / "out"
        args = argparse.Namespace(
            command="all",
            mapping=mapping_path,
            data_dir=data_dir,
            out_dir=out_dir,
            years=None,
            modules="FAM,WLTH",
            fail_fast=True,
            quiet=True,
            parallel=1,
            sample_rows=None,
            wide_only=False,
            long_only=False,
            coerce_types=True,
            apply_transforms=False,
            index_cols=None,
        )
        main(args)
        wide = pd.read_csv(out_dir / "mapping_dictionary_wide.csv")
        long_df = pd.read_csv(out_dir / "mapping_dictionary_long.csv")
        panel = pd.read_csv(out_dir / "panel_required_only.csv")
        assert "Total Income" in wide["label_en"].tolist()
        assert "Net Worth" in long_df["label_en"].tolist()
        assert panel["year"].tolist() == [1999, 1999, 2001]
    print("[SELFTEST OK]")


def main(parsed_args: Optional[argparse.Namespace] = None) -> None:
    """Main dispatcher for CLI commands."""

    if parsed_args is None:
        args = parse_args()
    else:
        args = parsed_args
    logger = configure_logging(args.quiet)
    years = parse_year_filters(args.years, logger, args.fail_fast)
    modules = parse_modules(args.modules)
    if not modules:
        raise ValueError("No valid modules specified; expected any of FAM, WLTH")

    if args.command == "dict" or args.command == "all":
        run_dict(
            args.mapping,
            args.data_dir,
            args.out_dir,
            years,
            modules,
            logger,
            wide_only=args.wide_only,
            long_only=args.long_only,
            fail_fast=args.fail_fast,
        )

    if args.command == "extract" or args.command == "all":
        index_cols = [col.strip() for col in args.index_cols.split(",")] if args.index_cols else None
        options = ExtractionOptions(
            apply_transforms=args.apply_transforms,
            coerce_types=args.coerce_types,
            sample_rows=args.sample_rows,
            fail_fast=args.fail_fast,
            quiet=args.quiet,
        )
        run_extract_cli(
            args.mapping,
            args.data_dir,
            args.out_dir,
            years,
            modules,
            logger,
            options,
            index_cols,
            args.parallel,
            args.fail_fast,
        )


if __name__ == "__main__":
    if os.environ.get("PSID_TOOL_SELFTEST") == "1":
        run_selftest()
    else:
        main()
