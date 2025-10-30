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

#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "     🚀 PSID PIPELINE — FULL RUN"
echo "======================================"

# -------- Paths / Inputs --------
DATA_DIR="sorted_data"          # Folder with raw converted *_full.csv
OUT_DIR="out"                   # Working directory for mapping + grids
FINAL_DIR="final_results"       # All final deliverables end up here
FILE_LIST="file_list.txt"       # List of SAS/TXT files for conversion (if used)
MERGE_GROUPS_FILE="merge_groups.txt"  # Optional: groups of grid rows to merge (stdin to merge_grid.py)

# -------- Helpers --------
ts() { date "+%Y-%m-%d %H:%M:%S"; }
ensure_dir() { mkdir -p "$1"; }

ensure_dir "$DATA_DIR"
ensure_dir "$OUT_DIR"
ensure_dir "$FINAL_DIR"

echo "[$(ts)] Using:"
echo "          DATA_DIR = $DATA_DIR"
echo "           OUT_DIR = $OUT_DIR"
echo "         FINAL_DIR = $FINAL_DIR"
echo "   MERGE_GROUPS_TXT = $MERGE_GROUPS_FILE"
echo

# ======================================
# 1) Convert SAS/TXT → CSV full (optional)
# ======================================
if [[ -f "$FILE_LIST" ]]; then
  echo "[$(ts)] [1/5] Converting SAS/TXT to CSV full from $FILE_LIST ..."
  python sas_to_csv.py --file-list "$FILE_LIST" --out-dir "$DATA_DIR"
  echo "[OK] CSV full files written to $DATA_DIR"
else
  echo "[$(ts)] [1/5] Skipping conversion (no $FILE_LIST found)."
fi
echo

# ======================================
# 2) Build mapping_long.csv
# ======================================
echo "[$(ts)] [2/5] Building mapping_long.csv from CSV headers ..."
python create_mapping.py --data-dir "$DATA_DIR" --out-dir "$OUT_DIR"
echo "[OK] Mapping written to $OUT_DIR/mapping_long.csv"
echo

# ======================================
# 3) Build canonical grid (variables × years)
#    -> produces $OUT_DIR/canonical_grid.csv
# ======================================
echo "[$(ts)] [3/5] Building canonical_grid.csv ..."
python psid_tool.py \
  --mapping "$OUT_DIR/mapping_long.csv" \
  --data-dir "$DATA_DIR" \
  --out-dir "$OUT_DIR" \
  --labels-source mapping \
  --prefer WLTH
echo "[OK] Wrote $OUT_DIR/canonical_grid.csv"
echo

# ======================================
# 4) Optional merge of rows in canonical grid
#    If merge_groups.txt exists, we merge by UNION and keep one value per cell.
#    -> produces $OUT_DIR/canonical_grid_merged.csv (if applied)
#    -> final grid copied to $OUT_DIR/final_grid.csv
# ======================================
FINAL_GRID="$OUT_DIR/final_grid.csv"
if [[ -f "$MERGE_GROUPS_FILE" ]]; then
  echo "[$(ts)] [4/5] Merging grid rows from $MERGE_GROUPS_FILE ..."
  python merge_grid.py --file "$OUT_DIR/canonical_grid.csv" --out "$OUT_DIR/canonical_grid_merged.csv" < "$MERGE_GROUPS_FILE"
  cp -f "$OUT_DIR/canonical_grid_merged.csv" "$FINAL_GRID"
  echo "[OK] Using merged grid as $FINAL_GRID"
else
  echo "[$(ts)] [4/5] No $MERGE_GROUPS_FILE; using canonical grid as final."
  cp -f "$OUT_DIR/canonical_grid.csv" "$FINAL_GRID"
fi
echo

# ======================================
# 5) Build final panel + per-family grids
#    -> panel_parent_child.csv, parent_child_links.csv, panel_summary.csv
#    -> panel_grid_by_family.csv (variables as rows, years as columns) + family_grids/
# ======================================
echo "[$(ts)] [5/5] Building final panel & family grids ..."
python build_panel_parent_child.py \
  --final-grid "$FINAL_GRID" \
  --mapping "$OUT_DIR/mapping_long.csv" \
  --data-dir "$DATA_DIR" \
  --out-dir "$FINAL_DIR" \
  --prefer WLTH \
  --write-family-files

echo
echo "======================================"
echo " ✅ DONE"
echo " Outputs in: $FINAL_DIR"
echo " - panel_parent_child.csv"
echo " - parent_child_links.csv"
echo " - panel_summary.csv"
echo " - panel_grid_by_family.csv"
echo " - family_grids/*.csv (one file per family)"
echo "======================================"