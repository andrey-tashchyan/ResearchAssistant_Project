#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
no_children_from_gid.py
-----------------------
À partir du fichier GID_full.csv (structure enfant ↔ parents),
ce script identifie toutes les personnes qui **n’ont jamais eu d’enfants**
sur la durée complète du panel PSID.

Entrée :
    sorted_data/GID_full.csv
      Colonnes attendues :
        ER30001          → Famille (interview ID) de l’individu
        ER30002          → Numéro de personne dans la famille
        ER30001_P_F      → Famille (interview ID) du père
        ER30002_P_F      → Numéro de personne du père
        ER30001_P_M      → Famille (interview ID) de la mère
        ER30002_P_M      → Numéro de personne de la mère

Sortie :
    out/no_children.csv
      Colonne | Description
      --------|-------------
      Family_ID (ER30001)           | Identifiant du foyer PSID d’origine
      Person_ID (ER30002)           | Identifiant individuel au sein du foyer
      Has_Children (0/1)            | 1 si la personne a au moins un enfant, 0 sinon
      Unique_PSID_ID                | Identifiant combiné famille-personne (utile pour les merges)
"""

from pathlib import Path
import pandas as pd


def main():
    gid_path = Path("sorted_data/GID_full.csv")
    out_dir = Path("out")
    out_dir.mkdir(exist_ok=True)

    # Lecture du fichier GID complet
    gid = pd.read_csv(gid_path, dtype=str)
    print(f"[OK] Lecture du GID : {len(gid):,} lignes, {len(gid.columns)} colonnes")

    # Normalisation de base
    for col in gid.columns:
        gid[col] = gid[col].astype("string").str.strip()

    # 1️⃣ Ensemble des individus présents (chaque ligne = un enfant)
    gid["child_id"] = gid["ER30001"].str.zfill(4) + "-" + gid["ER30002"].str.zfill(4)
    all_individuals = set(gid["child_id"].dropna())

    # 2️⃣ Ensemble des parents (pères + mères)
    gid["father_id"] = gid["ER30001_P_F"].fillna("").str.zfill(4) + "-" + gid["ER30002_P_F"].fillna("").str.zfill(4)
    gid["mother_id"] = gid["ER30001_P_M"].fillna("").str.zfill(4) + "-" + gid["ER30002_P_M"].fillna("").str.zfill(4)
    parents = set(gid["father_id"].dropna()) | set(gid["mother_id"].dropna())
    parents.discard("-")  # supprimer les vides

    print(f"[INFO] Nombre total d’individus : {len(all_individuals):,}")
    print(f"[INFO] Nombre total de parents identifiés : {len(parents):,}")

    # 3️⃣ Différence : individus qui ne figurent jamais comme père ou mère
    no_children = sorted(all_individuals - parents)
    print(f"[RESULT] Nombre de personnes sans enfants : {len(no_children):,}")

    # 4️⃣ Génération du DataFrame final
    df_no_children = pd.DataFrame(no_children, columns=["Unique_PSID_ID"])
    df_no_children[["Family_ID (ER30001)", "Person_ID (ER30002)"]] = df_no_children["Unique_PSID_ID"].str.split("-", expand=True)
    df_no_children["Has_Children (0=no,1=yes)"] = 0

    # Les autres (avec enfants)
    df_with_children = pd.DataFrame(sorted(all_individuals & parents), columns=["Unique_PSID_ID"])
    df_with_children[["Family_ID (ER30001)", "Person_ID (ER30002)"]] = df_with_children["Unique_PSID_ID"].str.split("-", expand=True)
    df_with_children["Has_Children (0=no,1=yes)"] = 1

    # Combine tout
    final_df = pd.concat([df_no_children, df_with_children], ignore_index=True)
    final_df = final_df.sort_values(["Family_ID (ER30001)", "Person_ID (ER30002)"]).reset_index(drop=True)
    final_df = final_df[["Family_ID (ER30001)", "Person_ID (ER30002)", "Has_Children (0=no,1=yes)", "Unique_PSID_ID"]]

    # Sauvegarde
    out_path = out_dir / "no_children.csv"
    final_df.to_csv(out_path, index=False)
    print(f"[OK] Fichier écrit : {out_path} ({len(final_df):,} lignes)")

    # Aperçu console
    print("\n🧩 Aperçu du fichier no_children.csv :")
    print(final_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()