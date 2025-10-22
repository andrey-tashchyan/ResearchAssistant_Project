nano run_all.sh#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "     🚀 PSID PIPELINE FULL LAUNCH"
echo "======================================"

# === Dossiers ===
DATA_DIR="sorted_data"   # .sas/.txt et *_full.csv
OUT_DIR="out"
FILE_LIST="file_list.txt"

# === Étape 1 : Conversion SAS/TXT → CSV full ===
echo "[1/3] Conversion SAS/TXT vers CSV full..."
python sas_to_csv.py --file-list "$FILE_LIST" --out-dir "$DATA_DIR"
echo "[OK] Fichiers CSV full créés dans $DATA_DIR"
echo

# === Étape 2 : Création du mapping ===
echo "[2/3] Création du mapping à partir des CSV full..."
python create_mapping.py --data-dir "$DATA_DIR" --out-dir "$OUT_DIR"
echo "[OK] Mapping généré dans $OUT_DIR/mapping_long.csv"
echo

# === Étape 3 : Extraction du panel harmonisé ===
echo "[3/3] Extraction du panel harmonisé..."
python psid_tool.py extract \
  --mapping "$OUT_DIR/mapping_long.csv" \
  --data-dir "$DATA_DIR" \
  --out-dir "$OUT_DIR"
echo "[OK] Panel harmonisé créé dans $OUT_DIR/panel_long.csv"
echo

echo "✅ PIPELINE TERMINÉ AVEC SUCCÈS !"
