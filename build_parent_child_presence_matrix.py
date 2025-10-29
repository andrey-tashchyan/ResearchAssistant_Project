#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_parent_child_presence_matrix.py
-------------------------------------
Construit une matrice de présence (parents × années) à partir :
- presence_panel_min.csv  (présence des individus par année)
- child_parent_links_nocanon.csv  (liens enfants ↔ parents)

Chaque ligne = un parent unique
Colonnes = années (0/1 si présent)
+ colonnes résumées sur les enfants : nb enfants, présence d'au moins un enfant dans le panel
"""

import pandas as pd
from pathlib import Path

# ==============================
# 1. CONFIG
# ==============================
DATA_DIR = Path("out")
PRES_PATH = DATA_DIR / "presence_panel_min.csv"
LINK_PATH = DATA_DIR / "child_parent_links_nocanon.csv"
OUT_PATH  = DATA_DIR / "parent_presence_matrix_enriched.csv"

# ==============================
# 2. LOAD DATA
# ==============================
print("[INFO] Lecture des fichiers...")
presence = pd.read_csv(PRES_PATH, dtype=str)
links = pd.read_csv(LINK_PATH, dtype=str)

presence["year"] = presence["year"].astype(int)
presence["person_value"] = presence["person_value"].astype(str)
links["father_value"] = links["father_value"].astype(str)
links["mother_value"] = links["mother_value"].astype(str)
links["child_value"] = links["child_value"].astype(str)

# ==============================
# 3. CONSTRUIRE LA MATRICE DE PRÉSENCE PARENTS × ANNÉES
# ==============================
print("[INFO] Construction de la matrice de présence des parents...")
# Liste des parents (pères + mères)
all_parents = pd.concat([links["father_value"], links["mother_value"]], ignore_index=True)
all_parents = all_parents.dropna().unique()

# Restreindre le roster aux parents
pres_parents = presence[presence["person_value"].isin(all_parents)].copy()
pres_parents["presence"] = 1

# Pivot wide : parent en ligne, année en colonne
parent_matrix = pres_parents.pivot_table(
    index="person_value", columns="year", values="presence", aggfunc="max", fill_value=0
).sort_index()

# ==============================
# 4. AJOUTER LES INFORMATIONS SUR LES ENFANTS
# ==============================
print("[INFO] Calcul des enfants par parent...")

# a) dictionnaires parent -> liste d'enfants
father_children = (
    links.dropna(subset=["father_value"])
    .groupby("father_value")["child_value"]
    .agg(list)
    .to_dict()
)
mother_children = (
    links.dropna(subset=["mother_value"])
    .groupby("mother_value")["child_value"]
    .agg(list)
    .to_dict()
)

# fusionner pères et mères (si un individu est à la fois père et mère, on prend la union)
all_children_map = {}
for parent, kids in {**father_children, **mother_children}.items():
    if parent not in all_children_map:
        all_children_map[parent] = set(kids)
    else:
        all_children_map[parent] = all_children_map[parent].union(kids)

# b) nombre total d’enfants
nb_children = {p: len(kids) for p, kids in all_children_map.items()}
parent_matrix["nb_children_total"] = parent_matrix.index.map(lambda x: nb_children.get(x, 0))

# c) présence d'au moins un enfant par année
years = [c for c in parent_matrix.columns if isinstance(c, int)]
print(f"[INFO] Années détectées: {years}")

for year in years:
    kids_present = set(presence.loc[presence["year"] == year, "person_value"])
    def has_child(p):
        return any(child in kids_present for child in all_children_map.get(p, []))
    parent_matrix[f"has_child_{year}"] = parent_matrix.index.map(lambda p: 1 if has_child(p) else 0)

# ==============================
# 5. STATISTIQUES & SAUVEGARDE
# ==============================
print(f"[OK] Matrice générée: {len(parent_matrix)} parents, {len(years)} années.")
parent_matrix.to_csv(OUT_PATH)
print(f"[OK] Fichier écrit -> {OUT_PATH}")