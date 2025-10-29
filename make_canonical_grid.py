#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
make_canonical_grid.py
----------------------
Construit une grille canonique (concept x années -> var_code) à partir de out/mapping_long.csv,
avec normalisation robuste des labels, tie-break déterministe, règles spécifiques AGE,
et fusions manuelles avant pivot.

Entrée :
  - out/mapping_long.csv  (colonnes min: year, var_code, label; optionnelles: file_type, category)

Sorties :
  - out/canonical_grid.csv
  - out/canonical_dict.csv
  - out/canonical_conflicts.csv
  - out/canonical_missing_by_year.csv
  - out/canonical_leftovers.csv
  - out/canonical_unmatched.csv

Options :
  --mapping PATH            (default: out/mapping_long.csv)
  --out-dir PATH            (default: out)
  --prefer WLTH|FAM         (priorité module hors cas spéciaux ; default: WLTH)
  --drop-imp                (filtre les labels contenant 'IMP')
  --years 1999,2001,...     (limite aux années listées)
"""

from __future__ import annotations
import argparse
import re
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

# ------------------- Regex / règles de nettoyage -------------------

WS_RE        = re.compile(r"\s+")
WAVE_RE      = re.compile(r"\(W\d+\)", re.IGNORECASE)          # (W6), (W22)...
YEAR2_RE     = re.compile(r"\b\d{2}\b")                        # 99, 01, 03
YEAR4_RE     = re.compile(r"\b(19|20)\d{2}\b")                 # 1999, 2011...
PUNCT_RE     = re.compile(r"[^\w\s/]+")                        # garde '/' pour annuity/ira
BRACKETS_RE  = re.compile(r"[\[\]\(\)\{\}]")
DIGIT_TOK_RE = re.compile(r"\b\d+\b")

# tokens à ignorer
STOP_TOKENS = {
    "imp", "acc", "wtr", "whether", "ever", "any",
    "of", "the", "a", "an", "and", "or", "to", "in", "for", "by",
    "head", "hh", "household"  # on traitera head séparément pour AGE
}

# Synonymes simples (en minuscules)
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
    "balance": "value",          # souvent même concept économique
    "account": "acct",
    "accounts": "acct",
    "mortgages": "mortgage",
}

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

# ------------------- Détection robuste de l'ÂGE -------------------
# Capture des variantes : "AGE OF HEAD", "AGE – REFERENCE PERSON", "AGE RESPONDENT", etc.

AGE_PATTERNS = [
    r"\bage\b.*\b(head|reference person|ref person|respondent|hd)\b",
    r"\b(head|reference person|ref person|respondent|hd)\b.*\bage\b",
    r"\bage of (head|reference person|ref person|respondent)\b",
]
AGE_SPOUSE_PATTERNS = [
    r"\bage\b.*\b(spouse|wife|husband|partner)\b",
    r"\b(spouse|wife|husband|partner)\b.*\bage\b",
    r"\bage of (spouse|wife|husband|partner)\b",
]

def is_match_any(s: str, patterns: list[str]) -> bool:
    s = s.lower()
    return any(re.search(p, s) for p in patterns)

def label_to_concept(label: str) -> str:
    if not isinstance(label, str) or not label.strip():
        return ""
    raw = label  # garder l'original pour les règles AGE
    s = normalize_phrase(label)
    tokens = []
    for t in s.split():
        if t in STOP_TOKENS:
            continue
        if DIGIT_TOK_RE.fullmatch(t):
            continue
        tokens.append(t)
    s2 = " ".join(tokens)
    s2 = apply_synonyms(s2)
    s2 = re.sub(r"\bvalue of\b", "value", s2)
    s2 = WS_RE.sub(" ", s2).strip()

    # Surclassement AGE (après normalisation mais basé sur label brut)
    raw_low = raw.lower()
    if is_match_any(raw_low, AGE_PATTERNS):
        return "demographics :: age_head"
    if is_match_any(raw_low, AGE_SPOUSE_PATTERNS):
        return "demographics :: age_spouse"

    return s2

# ------------------- Scoring / tie-break -------------------

def score_row(row: pd.Series, prefer_module: str, code_freq: Dict[str, int]) -> int:
    label   = str(row.get("label", "")).lower()
    var     = str(row.get("var_code", ""))
    mod     = str(row.get("file_type", "")).upper()
    concept = str(row.get("concept", ""))

    score = 0
    # ACC > IMP
    if "acc" in f" {label} ":
        score += 3
    if "imp" in f" {label} ":
        score -= 3
    # suffixe 'A' souvent la version ACC
    if var.endswith("A"):
        score += 2
    # 'value' > 'whether'
    if "value" in label:
        score += 1
    if "whether" in label or "wtr" in f" {label} ":
        score -= 1

    # Priorité module :
    #   - Forcer FAM pour les concepts d'AGE (plus stables)
    if concept.startswith("demographics :: age_"):
        if mod == "FAM":
            score += 5
    else:
        if prefer_module in {"WLTH", "FAM"} and mod == prefer_module:
            score += 1

    # informativité & stabilité du code à travers années
    score += min(len(label) // 40, 2)
    score += min(code_freq.get(var, 0), 2)
    return score

# ------------------- Fusions manuelles de concepts -------------------
# clé = concept à déplacer -> valeur = concept cible (appliqué AVANT pivot)
MANUAL_MERGES: Dict[str, str] = {
    # Merge demandé explicitement :
    "retirement/ira :: value vehicles": "fam/unknown :: value vehicles",
    # Exemple additionnel (si utile) : regrouper "fam/unknown :: value vehicles" sous un tronc commun
    # "fam/unknown :: value vehicles": "assets/debt :: value vehicles",
}

# ------------------- Pipeline principal -------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mapping", default="out/mapping_long.csv")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--prefer", default="WLTH", choices=["WLTH", "FAM"])
    ap.add_argument("--drop-imp", action="store_true")
    ap.add_argument("--years", default=None, help="Comma list, e.g. 1999,2001,2003,...")
    args = ap.parse_args()

    mapping_csv = Path(args.mapping)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(mapping_csv, dtype=str)

    # Colonnes minimales
    for c in ["year", "var_code", "label"]:
        if c not in df.columns:
            raise ValueError(f"Colonne manquante '{c}' dans {mapping_csv}")

    # Filtre années si demandé
    if args.years:
        keep_years = {y.strip() for y in args.years.split(",") if y.strip()}
        df = df[df["year"].astype(str).isin(keep_years)].copy()

    # Optionnel : drop IMP
    if args.drop_imp:
        df = df[~df["label"].str.contains(r"\bIMP\b", case=False, na=False)].copy()

    # Concept brut
    df["concept_base"] = df["label"].apply(label_to_concept)

    # Leftovers : labels vidés par normalisation
    leftovers = df[df["concept_base"].str.len() < 3].copy()
    if not leftovers.empty:
        leftovers.to_csv(out_dir / "canonical_leftovers.csv", index=False)
    df = df[df["concept_base"].str.len() >= 3].copy()

    # Ancre de catégorie si dispo
    if "category" in df.columns:
        df["concept"] = (df["category"].fillna("").str.lower().str.strip() + " :: " + df["concept_base"])
    else:
        df["concept"] = df["concept_base"]

    # Fusions manuelles AVANT scoring/pivot
    df["concept"] = df["concept"].replace(MANUAL_MERGES)

    # Fréquences de codes (stabilité)
    code_freq = df["var_code"].value_counts().to_dict()

    # Score pour tie-break
    df["score"] = df.apply(lambda r: score_row(r, args.prefer, code_freq), axis=1)
    df["file_type"] = df.get("file_type", "UNK")
    df["label_len"] = df["label"].fillna("").str.len()

    # Conflits avant tie-break (diagnostic)
    tmp = df.groupby(["concept", "year"]).size().reset_index(name="n")
    conflicts = tmp[tmp["n"] > 1]
    if not conflicts.empty:
        (df.merge(conflicts[["concept", "year"]], on=["concept", "year"], how="inner")
          .sort_values(["concept", "year", "score", "var_code"], ascending=[True, True, False, True])
          .to_csv(out_dir / "canonical_conflicts.csv", index=False))

    # Tie-break déterministe (pas de groupby.apply -> pas de FutureWarning)
    df = df.sort_values(
        by=["concept", "year", "score", "file_type", "label_len", "var_code"],
        ascending=[True, True, False, True, False, True]
    )
    picked = df.groupby(["concept", "year"], as_index=False).head(1).reset_index(drop=True)

    # Grille large
    grid = picked.pivot(index="concept", columns="year", values="var_code")
    try:
        grid = grid.reindex(sorted(grid.columns, key=lambda x: int(str(x))), axis=1)
    except Exception:
        grid = grid.sort_index(axis=1)

    grid_out = out_dir / "canonical_grid.csv"
    grid.to_csv(grid_out)
    print(f"[OK] wrote {grid_out} (rows={grid.shape[0]}, years={grid.shape[1]})")

    # Dictionnaire des concepts
    if "category" in picked.columns:
        ex = (picked.sort_values(["concept", "year"])
                    .groupby("concept", as_index=False)
                    .agg(example_label=("label", "first"),
                        any_file_type=("file_type", "first"),
                        any_category=("category", "first")))
    else:
        ex = (picked.sort_values(["concept", "year"])
                    .groupby("concept", as_index=False)
                    .agg(example_label=("label", "first"),
                        any_file_type=("file_type", "first")))
    dict_out = out_dir / "canonical_dict.csv"
    ex.to_csv(dict_out, index=False)
    print(f"[OK] wrote {dict_out} (rows={len(ex)})")

    # Couverture par année
    cov = picked.groupby("year").size().reset_index(name="n_concepts")
    cov_out = out_dir / "canonical_missing_by_year.csv"
    cov.to_csv(cov_out, index=False)
    print(f"[OK] wrote {cov_out}")

    # Concepts trop rares (<= 1 année) pour inspection
    rarity = picked.groupby("concept")["year"].nunique().reset_index(name="n_years")
    unmatched = rarity[rarity["n_years"] <= 1].merge(
        picked[["concept", "year", "var_code", "label", "file_type"]].drop_duplicates(),
        on="concept", how="left"
    ).sort_values(["n_years", "concept", "year"])
    if not unmatched.empty:
        unmatched.to_csv(out_dir / "canonical_unmatched.csv", index=False)
        print(f"[INFO] wrote {out_dir / 'canonical_unmatched.csv'} (rows={len(unmatched)})")

if __name__ == "__main__":
    main()