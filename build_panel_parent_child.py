#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_panel_parent_child.py — PSID pipeline → panel_grid_by_family.csv
----------------------------------------------------------------------
Objectif: construire, pour chaque FAMILLE, un tableau
Variables (lignes = concepts de final_grid.csv) × Années (1999..2023 en colonnes).

Entrées:
  - final_grid.csv (souvent: out/final_grid.csv) : grille canonique choisie
  - mapping_long.csv : dictionnaire PSID (var_code, year, module, etc.)
  - sorted_data/ : CSV PSID annuels (FAMyyyy*_full.csv, WLTHyyyy*_full.csv)

Sorties dans final_results/ :
  - panel_parent_child.csv            (panel long individuel, utile au contrôle)
  - parent_child_links.csv            (liens mère/père → enfant)
  - panel_summary.csv                 (couverture + stats basiques)
  - codes_resolved_audit.csv          (journal correspondances)
  - panel_grid_by_family.csv          (★ format final demandé, groupé par foyer)
  - family_grids/<family_id>.csv      (optionnel: un fichier par famille)
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# ------------ Logging -------------------------------------------------

def configure_logging(quiet: bool) -> logging.Logger:
    logger = logging.getLogger("psid_pipeline")
    if logger.handlers:
        logger.setLevel(logging.WARNING if quiet else logging.INFO)
        return logger
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.WARNING if quiet else logging.INFO)
    logger.setLevel(logging.WARNING if quiet else logging.INFO)
    return logger

# ------------ Utils ---------------------------------------------------

YEAR_RE = re.compile(r"^(19|20)\d{2}$")
MODULE_PREFIXES = {"FAM", "WLTH"}

def normalize(col: str) -> str:
    return str(col).strip()

def parse_years_from_grid(df: pd.DataFrame) -> List[int]:
    years = sorted(int(c) for c in df.columns if YEAR_RE.match(str(c)))
    return years

def ensure_outdir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path

# ------------ Loaders -------------------------------------------------

EXPECTED_MAPPING_COLS = [
    "canonical","year","file_type","var_code","label","category","dtype","required","transform"
]

def load_grid(path: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Loading grid: %s", path)
    df = pd.read_csv(path, dtype=str).fillna("")
    if "concept" not in df.columns:
        raise ValueError("final_grid.csv must contain a 'concept' column.")
    # garder l'ordre d’origine des concepts
    df["__order__"] = range(1, len(df) + 1)
    return df

def load_mapping(path: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Loading mapping: %s", path)
    df = pd.read_csv(path, dtype=str)
    missing = [c for c in EXPECTED_MAPPING_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"mapping_long.csv is missing columns: {missing}")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["file_type"] = df["file_type"].astype(str).str.upper()
    df["var_code"] = df["var_code"].astype(str).str.strip()
    df["label"] = df["label"].fillna("").astype(str)
    df["required"] = pd.to_numeric(df["required"], errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["year"]).reset_index(drop=True)

def gather_files(data_dir: Path, years: Set[int]) -> Dict[int, List[Path]]:
    files_by_year: Dict[int, List[Path]] = {}
    for p in sorted(data_dir.glob("*.csv")):
        name = p.name.upper()
        mod = next((m for m in MODULE_PREFIXES if name.startswith(m)), None)
        if not mod:
            continue
        m = re.search(r"(19|20)\d{2}", name)
        if not m:
            continue
        y = int(m.group(0))
        if y not in years:
            continue
        files_by_year.setdefault(y, []).append(p)
    for y in files_by_year:
        files_by_year[y] = sorted(files_by_year[y], key=lambda x: x.name)
    return dict(sorted(files_by_year.items()))

# ------------ Resolve codes from final_grid ---------------------------

def extract_required_concepts(grid: pd.DataFrame) -> pd.DataFrame:
    # garder seulement required==1 si la colonne existe
    if "required" in grid.columns:
        mask = pd.to_numeric(grid["required"], errors="coerce").fillna(0).astype(int) == 1
        out = grid[mask].copy()
        # s'assurer que les IDs passent même si non marqués
        id_like = grid["concept"].str.lower().str.contains(
            r"\b(family_id|person_id|mother_id|father_id)\b"
        )
        out = pd.concat([out, grid[id_like & ~mask]], axis=0).drop_duplicates("concept")
        return out.sort_values("__order__")
    return grid.sort_values("__order__").copy()

def resolve_codes_from_grid(grid: pd.DataFrame) -> Tuple[List[int], Dict[str, Dict[int, str]]]:
    years = parse_years_from_grid(grid)
    code_map: Dict[str, Dict[int, str]] = {}
    for _, row in grid.iterrows():
        concept = str(row["concept"])
        code_map[concept] = {}
        for y in years:
            val = str(row.get(str(y), "")).strip()
            code_map[concept][y] = val if val else ""
    return years, code_map

# ------------ Map (year, code) -> module/file ------------------------

def map_codes_to_module(mapping: pd.DataFrame,
                        code_map: Dict[str, Dict[int, str]],
                        prefer: str,
                        logger: logging.Logger) -> Tuple[Dict[int, Dict[str, Tuple[str, str]]], pd.DataFrame]:
    """
    Retourne, pour chaque année, pour chaque code, (module, filename_pattern).
    Audit: DataFrame listant concept,year,var_code,file_type.
    """
    prefer = prefer.upper()
    audit_rows = []
    by_year: Dict[int, Dict[str, Tuple[str, str]]] = {}

    for concept, per_year in code_map.items():
        for year, var_code in per_year.items():
            if not var_code:
                continue
            sub = mapping[(mapping["year"] == year) & (mapping["var_code"] == var_code)]
            if sub.empty:
                logger.warning("No mapping for (%s, %s, %s)", concept, year, var_code)
                continue
            # choix module preferé s'il y a doublon
            if len(sub["file_type"].unique()) > 1 and prefer in set(sub["file_type"]):
                sub = sub[sub["file_type"] == prefer]
            file_type = sub.iloc[0]["file_type"]
            module_prefix = "FAM" if file_type == "FAM" else "WLTH"
            filename_pattern = f"{module_prefix}{year}"
            by_year.setdefault(year, {})[var_code] = (file_type, filename_pattern)
            audit_rows.append({"concept": concept, "year": year, "var_code": var_code,
                               "file_type": file_type})

    audit = pd.DataFrame(audit_rows).sort_values(["year","concept","var_code"])
    return by_year, audit

# ------------ Extraction par année -----------------------------------

# Très courants dans PSID (fallback si la grille n’a pas fourni d’IDs)
FALLBACK_FAMILY_IDS = ["ER30001", "ER32000", "ER30000"]
FALLBACK_PERSON_IDS = ["ER30002", "ER32001"]

def select_id_concepts(concepts: List[str]) -> Dict[str, str]:
    """
    Détecte les 4 IDs conceptuels.
    """
    out: Dict[str, str] = {}
    for c in concepts:
        cl = c.lower()
        if "family_id" in cl and "family_id" not in out:
            out["family_id"] = c
        elif "person_id" in cl and "person_id" not in out:
            out["person_id"] = c
        elif "mother_id" in cl and "mother_id" not in out:
            out["mother_id"] = c
        elif "father_id" in cl and "father_id" not in out:
            out["father_id"] = c
    return out

def find_code_for_concept(concept: str, year: int, code_map: Dict[str, Dict[int, str]]) -> Optional[str]:
    return code_map.get(concept, {}).get(year, "") or None

def detect_fallback_ids(header: List[str]) -> Dict[str, str]:
    """Cherche ER30001/ER30002 (et variantes) si les IDs ne sont pas fournis."""
    found: Dict[str, str] = {}
    for cand in FALLBACK_FAMILY_IDS:
        if cand in header:
            found["family_id"] = cand
            break
    for cand in FALLBACK_PERSON_IDS:
        if cand in header:
            found["person_id"] = cand
            break
    return found

def read_year_data(year: int,
                   files: List[Path],
                   needed_codes: Set[str],
                   rename_to_concept: Dict[str, str],
                   id_code_pref: List[str],
                   logger: logging.Logger) -> pd.DataFrame:
    frames = []
    found_id_cols: List[str] = []
    fallback_seen: Dict[str, str] = {}

    for path in files:
        try:
            head = pd.read_csv(path, nrows=0)
        except Exception as e:
            logger.warning("Cannot read header of %s: %s", path.name, e)
            continue
        header = [normalize(c) for c in head.columns]

        # si aucun id spécifié, tenter fallback ER30001/ER30002
        if not id_code_pref:
            fb = detect_fallback_ids(header)
            # garder le premier fallback détecté globalement
            for k, v in fb.items():
                if k not in fallback_seen:
                    fallback_seen[k] = v

        present = [c for c in header if c in needed_codes]
        id_candidates = [c for c in header if c in (id_code_pref or list(fallback_seen.values()))]
        usecols = list(dict.fromkeys(present + id_candidates))
        if not usecols:
            continue
        try:
            df = pd.read_csv(path, usecols=usecols, low_memory=False)
        except Exception as e:
            logger.warning("Cannot read %s: %s", path.name, e)
            continue
        df.columns = [normalize(c) for c in df.columns]
        frames.append(df)
        found_id_cols.extend([c for c in id_candidates if c in df.columns])

    if not frames:
        return pd.DataFrame()

    # clés d’identification disponibles
    id_keys = []
    for c in (id_code_pref or []):
        if c in found_id_cols and c not in id_keys:
            id_keys.append(c)
    # ajouter fallback si on n’a rien
    if not id_keys:
        for c in fallback_seen.values():
            if c in found_id_cols and c not in id_keys:
                id_keys.append(c)

    if not id_keys:
        merged = pd.concat(frames, axis=1)
    else:
        merged = frames[0]
        for df in frames[1:]:
            common = [c for c in id_keys if c in merged.columns and c in df.columns]
            if not common:
                merged = pd.concat([merged, df], axis=1)
            else:
                merged = pd.merge(merged, df, on=common, how="inner")

    # renommer codes de variables → concepts
    ren = {code: concept for code, concept in rename_to_concept.items() if code in merged.columns}
    merged = merged.rename(columns=ren)
    merged.insert(0, "year", year)

    # Renommer les IDs (codes) → noms standards si présents
    id_ren: Dict[str, str] = {}
    # préférer id_code_pref
    for key, pref_list in {
        "family_id": id_code_pref,
        "person_id": id_code_pref,
    }.items():
        # map déjà géré ci-dessus; on s'appuie sur id_code_pref exacts
        pass
    # si fallback détecté, renommer
    for role, code in fallback_seen.items():
        if code in merged.columns:
            id_ren[code] = role
    if id_ren:
        merged = merged.rename(columns=id_ren)

    # colonnes ID minimales
    for k in ["family_id","person_id","mother_id","father_id"]:
        if k not in merged.columns:
            merged[k] = pd.NA

    return merged

# ------------ Parent / enfant & filtrage familles --------------------

def build_parent_child_links(panel: pd.DataFrame) -> pd.DataFrame:
    cols = ["year","family_id","person_id","mother_id","father_id"]
    for c in cols:
        if c not in panel.columns:
            panel[c] = pd.NA
    if panel.empty:
        return panel[cols].copy()
    links = panel[cols].dropna(subset=["person_id"]).copy()
    if links.empty:
        return links
    links["is_parent"] = links["person_id"].isin(links["mother_id"].dropna().unique()) | \
                         links["person_id"].isin(links["father_id"].dropna().unique())
    return links

def filter_families_with_children(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel
    needed = {"family_id","person_id","mother_id","father_id"}
    for c in needed:
        if c not in panel.columns:
            panel[c] = pd.NA
    kids = panel[panel["mother_id"].notna() | panel["father_id"].notna()]
    if kids.empty:
        # pas d’enfants — on retourne panel tel quel mais on ne plante pas
        return panel.copy()
    fam_keep = set(kids["family_id"].dropna().unique().tolist())
    return panel[panel["family_id"].isin(fam_keep)].copy()

# ------------ Résumé & stats -----------------------------------------

def summarize_panel(panel: pd.DataFrame,
                    concepts: List[str]) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame(columns=["concept","non_missing","mean","median","std"])
    rows = []
    for c in concepts:
        if c not in panel.columns:
            continue
        s = panel[c]
        nonmiss = int(s.notna().sum())
        rows.append({
            "concept": c,
            "non_missing": nonmiss,
            "mean": pd.to_numeric(s, errors="coerce").mean(skipna=True),
            "median": pd.to_numeric(s, errors="coerce").median(skipna=True),
            "std": pd.to_numeric(s, errors="coerce").std(skipna=True),
        })
    return pd.DataFrame(rows).sort_values("concept")

# ------------ Construction du grid par FAMILLE ----------------------

def write_per_family_grid(final_grid_path: Path,
                          panel_path: Path,
                          out_dir: Path,
                          write_individual_files: bool,
                          logger: logging.Logger) -> Path:
    """
    Construit panel_grid_by_family.csv :
    index (lignes) = (family_id, concept) — concepts dans l'ordre du final_grid
    colonnes = années (1999..2023)
    Règle d'agrégation par famille-année-concept: on prend la première valeur non manquante,
    en priorisant les PARENTS (si déductibles), sinon n'importe quel membre.
    """
    fg = pd.read_csv(final_grid_path, dtype=str).fillna("")
    if "concept" not in fg.columns:
        raise ValueError("final_grid.csv must contain a 'concept' column.")
    fg["__order__"] = range(1, len(fg) + 1)
    # garder required==1 + IDs (si existe)
    if "required" in fg.columns:
        mask_req = pd.to_numeric(fg["required"], errors="coerce").fillna(0).astype(int) == 1
        id_like = fg["concept"].str.lower().str.contains(r"\b(family_id|person_id|mother_id|father_id)\b")
        fg = pd.concat([fg[mask_req], fg[id_like & ~mask_req]], axis=0).drop_duplicates("concept")
    fg = fg.sort_values("__order__")
    concepts_order = fg["concept"].tolist()
    years = sorted(int(c) for c in fg.columns if YEAR_RE.match(str(c)))

    # si le panel n’existe pas ou vide, on écrit un squelette vide
    try:
        panel = pd.read_csv(panel_path, dtype=str).replace({"": pd.NA})
    except Exception:
        panel = pd.DataFrame(columns=["family_id","person_id","mother_id","father_id","year"] + concepts_order)

    if panel.empty:
        columns = ["family_id","concept"] + [str(y) for y in years]
        out_path = out_dir / "panel_grid_by_family.csv"
        pd.DataFrame(columns=columns).to_csv(out_path, index=False)
        logger.info("Wrote %s (empty scaffold)", out_path.name)
        return out_path

    # si family_id complètement manquant, affecter UNKNOWN_FAMILY pour éviter crash
    if "family_id" not in panel.columns:
        panel["family_id"] = pd.NA
    if panel["family_id"].isna().all():
        panel["family_id"] = "UNKNOWN_FAMILY"

    # flags parent (si identifiables)
    parents_ids = set(panel.get("mother_id", pd.Series(dtype=str)).dropna().tolist()) | \
                  set(panel.get("father_id", pd.Series(dtype=str)).dropna().tolist())
    panel["is_parent"] = panel.get("person_id", pd.Series(dtype=str)).isin(parents_ids)

    records = []
    # groupe par famille
    for family_id, fam in panel.groupby("family_id", dropna=False):
        fam_par = fam[fam["is_parent"] == True]
        # pour chaque concept, année -> choisit valeur
        for concept in concepts_order:
            row_vals = {}
            for y in years:
                sub = fam[fam["year"].astype(str) == str(y)]
                val = pd.NA
                if concept in sub.columns:
                    # priorité: parents -> sinon autres
                    if not fam_par.empty and concept in fam_par.columns:
                        s = sub[sub["is_parent"] == True][concept]
                        if not s.empty and s.notna().any():
                            val = s.dropna().iloc[0]
                    if pd.isna(val):
                        s2 = sub[concept]
                        if not s2.empty and s2.notna().any():
                            val = s2.dropna().iloc[0]
                row_vals[str(y)] = (val if pd.notna(val) else "")
            rec = {"family_id": family_id if pd.notna(family_id) else "",
                   "concept": concept}
            rec.update(row_vals)
            records.append(rec)

        # option: un fichier par famille
        if write_individual_files:
            small = pd.DataFrame([r for r in records if r["family_id"] == family_id])
            small = small[["concept"] + [str(y) for y in years]]
            fam_dir = out_dir / "family_grids"
            fam_dir.mkdir(parents=True, exist_ok=True)
            (fam_dir / f"{family_id}.csv").write_text(small.to_csv(index=False))

    grid_df = pd.DataFrame(records)
    # si pour une raison quelconque records est vide, écrire squelette
    if grid_df.empty:
        columns = ["family_id","concept"] + [str(y) for y in years]
        grid_df = pd.DataFrame(columns=columns)

    grid_df = grid_df[["family_id","concept"] + [str(y) for y in years]]
    out_path = out_dir / "panel_grid_by_family.csv"
    grid_df.to_csv(out_path, index=False)
    logger.info("Wrote %s (%d rows, %d cols)", out_path.name, len(grid_df), grid_df.shape[1])
    return out_path

# ------------ Pipeline principal -------------------------------------

def main():
    ap = argparse.ArgumentParser(description="PSID pipeline → panel_grid_by_family.csv")
    ap.add_argument("--final-grid", type=Path, required=True, help="Path to final_grid.csv (e.g., out/final_grid.csv)")
    ap.add_argument("--mapping", type=Path, required=True, help="Path to mapping_long.csv")
    ap.add_argument("--data-dir", type=Path, required=True, help="Directory with PSID CSV files")
    ap.add_argument("--out-dir", type=Path, required=True, help="Directory to write final_results")
    ap.add_argument("--prefer", choices=["WLTH","FAM"], default="WLTH")
    ap.add_argument("--coerce-types", action="store_true")
    ap.add_argument("--apply-transforms", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--write-family-files", action="store_true",
                    help="Also write one CSV per family into final_results/family_grids/")
    args = ap.parse_args()

    logger = configure_logging(args.quiet)
    final_results = ensure_outdir(Path(args.out_dir))  # all outputs go here

    # 1) Charger grille & mapping
    grid0 = load_grid(args.final_grid, logger)
    years_all = parse_years_from_grid(grid0)
    grid = extract_required_concepts(grid0)  # garde required==1 + IDs
    years, code_map = resolve_codes_from_grid(grid)

    mapping = load_mapping(args.mapping, logger)
    files_by_year = gather_files(args.data_dir, set(years))
    if not files_by_year:
        # on crée quand même des fichiers vides mais cohérents
        (final_results / "codes_resolved_audit.csv").write_text("")
        (final_results / "parent_child_links.csv").write_text("")
        (final_results / "panel_parent_child.csv").write_text("")
        (final_results / "panel_summary.csv").write_text("")
        write_per_family_grid(args.final_grid, final_results / "panel_parent_child.csv",
                              final_results, args.write_family_files, logger)
        logger.warning("No PSID module files found in data-dir for the requested years.")
        return

    # 2) Identifier codes→modules
    by_year_module, audit = map_codes_to_module(mapping, code_map, args.prefer, logger)
    audit_path = final_results / "codes_resolved_audit.csv"
    audit.to_csv(audit_path, index=False)
    logger.info("Wrote %s", audit_path.name)

    # 3) Déterminer concepts d'ID
    concepts_list = list(code_map.keys())
    id_concepts = select_id_concepts(concepts_list)
    # construire liste des codes d'ID par année s'ils existent dans la grille
    id_codes_by_year: Dict[int, Dict[str, str]] = {}
    for y in years:
        tmp = {}
        for idk, cname in id_concepts.items():
            code = find_code_for_concept(cname, y, code_map)
            if code:
                tmp[idk] = code
        id_codes_by_year[y] = tmp

    # 4) Extraction par année
    yearly_frames = []
    for y in years:
        if y not in files_by_year:
            logger.warning("No files for year %s", y)
            continue

        codes_needed: Set[str] = set()
        rename_to_concept: Dict[str, str] = {}
        for concept, per_year in code_map.items():
            code = per_year.get(y, "")
            if code:
                codes_needed.add(code)
                rename_to_concept[code] = concept

        id_pref = []
        for key in ["family_id","person_id","mother_id","father_id"]:
            c = id_codes_by_year.get(y, {}).get(key)
            if c:
                id_pref.append(c)

        dfy = read_year_data(
            y, files_by_year[y], codes_needed, rename_to_concept, id_pref, logger
        )
        if dfy.empty:
            logger.warning("Year %s produced no data after selection.", y)
            continue

        # Renommer les IDs (codes explicites) → noms standards si présents
        id_ren = {}
        for key in ["family_id","person_id","mother_id","father_id"]:
            code = id_codes_by_year.get(y, {}).get(key)
            if code and code in dfy.columns:
                id_ren[code] = key
        if id_ren:
            dfy = dfy.rename(columns=id_ren)

        for k in ["family_id","person_id","mother_id","father_id"]:
            if k not in dfy.columns:
                dfy[k] = pd.NA

        yearly_frames.append(dfy)

    # 5) Concat panel long et filtrer familles avec enfants
    if yearly_frames:
        panel = pd.concat(yearly_frames, axis=0, ignore_index=True)
        concept_cols = [c for c in panel.columns if c not in {"year","family_id","person_id","mother_id","father_id"}]
        panel = panel[["year","family_id","person_id","mother_id","father_id"] + concept_cols]
    else:
        panel = pd.DataFrame(columns=["year","family_id","person_id","mother_id","father_id"])

    panel_filtered = filter_families_with_children(panel)

    # 6) Liens parent-enfant
    links = build_parent_child_links(panel_filtered)
    links_path = final_results / "parent_child_links.csv"
    links.to_csv(links_path, index=False)
    logger.info("Wrote %s (%d rows)", links_path.name, len(links))

    # 7) Sauvegarde panel long
    panel_path = final_results / "panel_parent_child.csv"
    panel_filtered.to_csv(panel_path, index=False)
    logger.info("Wrote %s (%d rows, %d cols)", panel_path.name, len(panel_filtered), panel_filtered.shape[1])

    # 8) Stats/résumé
    summary = summarize_panel(panel_filtered, [c for c in panel_filtered.columns if c not in {"year","family_id","person_id","mother_id","father_id"}])
    summary_path = final_results / "panel_summary.csv"
    summary.to_csv(summary_path, index=False)
    logger.info("Wrote %s", summary_path.name)

    # 9) ★ Fichier final : grid par FAMILLE
    write_per_family_grid(args.final_grid, panel_path, final_results, args.write_family_files, logger)

if __name__ == "__main__":
    main()