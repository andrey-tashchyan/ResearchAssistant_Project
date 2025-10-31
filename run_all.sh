#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# PSID PIPELINE — Instrumented for Maximum Observability
# ============================================================================

# -------- Color Helpers --------
color() {
  case "$1" in
    CYAN)    echo -e "\033[0;36m$2\033[0m" ;;
    MAGENTA) echo -e "\033[0;35m$2\033[0m" ;;
    GREEN)   echo -e "\033[0;32m$2\033[0m" ;;
    BLUE)    echo -e "\033[0;34m$2\033[0m" ;;
    YELLOW)  echo -e "\033[0;33m$2\033[0m" ;;
    RED)     echo -e "\033[0;31m$2\033[0m" ;;
    GRAY)    echo -e "\033[0;90m$2\033[0m" ;;
    *)       echo "$2" ;;
  esac
}

# Verbosity control
VERBOSE="${VERBOSE:-1}"
QUIET="${QUIET:-0}"

log_title()   { [[ "$QUIET" == "0" ]] && color CYAN "[TITLE] $*" || true; }
log_step()    { color MAGENTA "[STEP] $*"; }
log_info()    { [[ "$VERBOSE" == "1" ]] && color GREEN "[INFO] $*" || true; }
log_file()    { [[ "$VERBOSE" == "1" ]] && color GRAY "[FILE] $*" || true; }
log_skip()    { [[ "$VERBOSE" == "1" ]] && color YELLOW "[SKIP] $*" || true; }
log_warn()    { color YELLOW "[WARN] $*"; }
log_ok()      { color GREEN "[OK] $*"; }
log_done()    { color GREEN "[DONE] $*"; }
log_time()    { color BLUE "[TIME] $*"; }
log_error()   { color RED "[ERROR] $*"; }

# High-resolution timer
timer_start() { TIMER_START=$(date +%s%N 2>/dev/null || date +%s); }
timer_end() {
  local start="$1"
  local label="$2"
  local end=$(date +%s%N 2>/dev/null || date +%s)
  if [[ "$end" =~ N ]]; then
    # Fallback to seconds only
    local elapsed=$((end - start))
    log_time "$label in ${elapsed}s"
  else
    # Nanosecond precision
    local elapsed=$(( (end - start) / 1000000 ))
    local sec=$((elapsed / 1000))
    local ms=$((elapsed % 1000))
    printf "$(color BLUE "[TIME] %s in %d.%03ds")\n" "$label" "$sec" "$ms"
  fi
}

# -------- Paths / Inputs --------
DATA_DIR="sorted_data"
OUT_DIR="out"
FINAL_DIR="final_results"
FILE_LIST="file_list.txt"
MERGE_GROUPS_FILE="merge_groups.txt"

# -------- Start Banner --------
PIPELINE_START=$(date +%s%N 2>/dev/null || date +%s)
RUN_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo ""
log_title "═══════════════════════════════════════════════════════════════"
log_title "    🚀 PSID PIPELINE — FULL RUN (Instrumented)"
log_title "═══════════════════════════════════════════════════════════════"
log_info "Run started: $RUN_TIMESTAMP"
log_info "Configuration:"
log_file "  DATA_DIR         = $DATA_DIR"
log_file "  OUT_DIR          = $OUT_DIR"
log_file "  FINAL_DIR        = $FINAL_DIR"
log_file "  FILE_LIST        = $FILE_LIST"
log_file "  MERGE_GROUPS_TXT = $MERGE_GROUPS_FILE"
log_info "Pipeline steps: [1] CSV Conversion → [2] Mapping Build → [3] Canonical Grid → [4] Row Merge → [5] Final Panel"
echo ""

# Ensure directories exist
log_step "Ensuring output directories exist..."
mkdir -p "$DATA_DIR" "$OUT_DIR" "$FINAL_DIR"
log_ok "Directories ready"
echo ""

# ============================================================================
# STEP 1: Convert SAS/TXT → CSV full (optional)
# ============================================================================
log_step "[1/5] SAS/TXT → CSV Conversion"
STEP1_START=$(date +%s%N 2>/dev/null || date +%s)

if [[ -f "$FILE_LIST" ]]; then
  log_info "Found file list: $FILE_LIST"
  FILE_COUNT=$(wc -l < "$FILE_LIST" | tr -d ' ')
  log_info "Files to process: $FILE_COUNT"

  python sas_to_csv.py --file-list "$FILE_LIST" --out-dir "$DATA_DIR"
  STEP1_EXIT=$?

  if [[ $STEP1_EXIT -ne 0 ]]; then
    log_error "Step 1 failed with exit code $STEP1_EXIT"
    exit $STEP1_EXIT
  fi

  log_ok "CSV conversion complete"
else
  log_skip "No $FILE_LIST found, skipping conversion"
fi

timer_end "$STEP1_START" "Step 1 (CSV Conversion)"
echo ""

# ============================================================================
# STEP 2: Build mapping_long.csv
# ============================================================================
log_step "[2/5] Building mapping_long.csv from CSV headers"
STEP2_START=$(date +%s%N 2>/dev/null || date +%s)

log_file "Input: $DATA_DIR/*.csv"
log_file "Output: $OUT_DIR/mapping_long.csv, $OUT_DIR/fam_wlth_inventory.csv"

python create_mapping.py --data-dir "$DATA_DIR" --out-dir "$OUT_DIR"
STEP2_EXIT=$?

if [[ $STEP2_EXIT -ne 0 ]]; then
  log_error "Step 2 failed with exit code $STEP2_EXIT"
  exit $STEP2_EXIT
fi

log_ok "Mapping written to $OUT_DIR/mapping_long.csv"
timer_end "$STEP2_START" "Step 2 (Mapping Build)"
echo ""

# ============================================================================
# STEP 3: Build canonical grid (variables × years)
# ============================================================================
log_step "[3/5] Building canonical_grid.csv"
STEP3_START=$(date +%s%N 2>/dev/null || date +%s)

log_file "Input: $OUT_DIR/mapping_long.csv"
log_file "Output: $OUT_DIR/canonical_grid.csv"
log_info "Strategy: prefer WLTH module for conflicts"

python psid_tool.py \
  --mapping "$OUT_DIR/mapping_long.csv" \
  --out-dir "$OUT_DIR" \
  --prefer WLTH

STEP3_EXIT=$?

if [[ $STEP3_EXIT -ne 0 ]]; then
  log_error "Step 3 failed with exit code $STEP3_EXIT"
  exit $STEP3_EXIT
fi

log_ok "Canonical grid written to $OUT_DIR/canonical_grid.csv"
timer_end "$STEP3_START" "Step 3 (Canonical Grid)"
echo ""

# ============================================================================
# STEP 4: Optional merge of rows in canonical grid
# ============================================================================
log_step "[4/5] Merging grid rows (optional)"
STEP4_START=$(date +%s%N 2>/dev/null || date +%s)

FINAL_GRID="$OUT_DIR/final_grid.csv"

if [[ -f "$MERGE_GROUPS_FILE" ]]; then
  log_info "Found merge groups file: $MERGE_GROUPS_FILE"
  MERGE_GROUP_COUNT=$(grep -c '^' "$MERGE_GROUPS_FILE" || echo 0)
  log_info "Merge groups to process: $MERGE_GROUP_COUNT"

  log_file "Input: $OUT_DIR/canonical_grid.csv"
  log_file "Output: $OUT_DIR/canonical_grid_merged.csv → $FINAL_GRID"

  python merge_grid.py \
    --file "$OUT_DIR/canonical_grid.csv" \
    --out "$OUT_DIR/canonical_grid_merged.csv" \
    < "$MERGE_GROUPS_FILE"

  STEP4_EXIT=$?

  if [[ $STEP4_EXIT -ne 0 ]]; then
    log_error "Step 4 failed with exit code $STEP4_EXIT"
    exit $STEP4_EXIT
  fi

  cp -f "$OUT_DIR/canonical_grid_merged.csv" "$FINAL_GRID"
  log_ok "Merged grid written to $FINAL_GRID"
else
  log_skip "No $MERGE_GROUPS_FILE found, using canonical grid as final"
  cp -f "$OUT_DIR/canonical_grid.csv" "$FINAL_GRID"
  log_file "Copied: $OUT_DIR/canonical_grid.csv → $FINAL_GRID"
fi

timer_end "$STEP4_START" "Step 4 (Row Merge)"
echo ""

# ============================================================================
# STEP 5: Build final panel with memory-efficient Parquet output
# ============================================================================
log_step "[5/5] Building final panel (memory-efficient Parquet)"
STEP5_START=$(date +%s%N 2>/dev/null || date +%s)

log_file "Input: $OUT_DIR/final_grid.csv, $OUT_DIR/mapping_long.csv"
log_file "Data: $DATA_DIR/*.csv"
log_file "Output: $FINAL_DIR/panel.parquet/ (partitioned, zstd compressed)"

python build_final_panel.py \
  --data-dir "$DATA_DIR" \
  --out-dir "$OUT_DIR" \
  --final-dir "$FINAL_DIR" \
  --rebuild

STEP5_EXIT=$?

if [[ $STEP5_EXIT -ne 0 ]]; then
  log_error "Step 5 failed with exit code $STEP5_EXIT"
  exit $STEP5_EXIT
fi

log_ok "Final panel written to $FINAL_DIR/panel.parquet/"
timer_end "$STEP5_START" "Step 5 (Final Panel)"
echo ""

# ============================================================================
# Final Summary Banner
# ============================================================================
PIPELINE_END=$(date +%s%N 2>/dev/null || date +%s)

echo ""
log_title "═══════════════════════════════════════════════════════════════"
log_title "    ✅ PIPELINE COMPLETE"
log_title "═══════════════════════════════════════════════════════════════"

# Calculate total runtime
if [[ "$PIPELINE_END" =~ N ]]; then
  TOTAL_SEC=$((PIPELINE_END - PIPELINE_START))
  log_time "Total runtime: ${TOTAL_SEC}s"
else
  TOTAL_MS=$(( (PIPELINE_END - PIPELINE_START) / 1000000 ))
  TOTAL_SEC=$((TOTAL_MS / 1000))
  TOTAL_MS_FRAC=$((TOTAL_MS % 1000))
  printf "$(color BLUE "[TIME] Total runtime: %d.%03ds")\n" "$TOTAL_SEC" "$TOTAL_MS_FRAC"
fi

log_info "Outputs:"
log_file "  → $OUT_DIR/mapping_long.csv"
log_file "  → $OUT_DIR/canonical_grid.csv"
log_file "  → $OUT_DIR/final_grid.csv"
log_file "  → $FINAL_DIR/panel.parquet/"

echo ""
log_info "To read the panel in Python:"
color GRAY "  import pandas as pd"
color GRAY "  df = pd.read_parquet('$FINAL_DIR/panel.parquet')"

echo ""
log_info "To query specific years:"
color GRAY "  df = pd.read_parquet('$FINAL_DIR/panel.parquet', filters=[('year', '==', 2009)])"

echo ""
log_done "Pipeline finished successfully at $(date "+%Y-%m-%d %H:%M:%S")"
log_title "═══════════════════════════════════════════════════════════════"
echo ""

exit 0
