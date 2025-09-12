# sas_to_csv.py
# (Corrections minimales : gestion de chemins vers sorted_data + file_list.txt)

import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
import pandas as pd

# --- Chemins (AJOUT MINIMAL) ---
ROOT_DIR = Path(__file__).parent.resolve()
DATA_DIR = ROOT_DIR / "sorted_data"          # ← là où sont les .sas/.txt
LIST_FILE = ROOT_DIR / "file_list.txt"       # ← ta liste de noms

# ---------------------------
# 1) Ta fonction existante
# ---------------------------
def txt_to_csv_full(txt_path: str, layout_path: str, csv_path: str) -> None:
    pat_input = re.compile(r'([A-Za-z0-9_]+)\s+(\d+)\s*-\s*(\d+)')
    pat_label = re.compile(r'^\s*([A-Za-z0-9_]+)\s+LABEL="([^"]+)"', re.IGNORECASE)

    raw_cols = []
    labels = {}

    text = Path(layout_path).read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in text:
        for var, a, b in pat_input.findall(line):
            raw_cols.append((var, int(a), int(b)))
        m = pat_label.match(line)
        if m:
            var, lab = m.groups()
            labels[var] = lab

    if not raw_cols:
        raise ValueError("Aucun couple VAR start-end détecté dans le layout.")

    raw_cols.sort(key=lambda t: t[1])

    names = [t[0] for t in raw_cols]
    counts = Counter(names)
    seen = Counter()
    colnames = []
    for name in names:
        seen[name] += 1
        colnames.append(f"{name}_{seen[name]}" if counts[name] > 1 else name)

    colspecs = [(start - 1, end) for _, start, end in raw_cols]

    expected_len = max(end for _, _, end in raw_cols)
    with open(txt_path, 'r', encoding='utf-8', errors='ignore') as ftxt:
        for first in ftxt:
            if first.strip():
                actual_len = len(first.rstrip("\n\r"))
                if actual_len < expected_len:
                    raise ValueError(f"Ligne trop courte ({actual_len} < {expected_len}).")
                break

    df = pd.read_fwf(txt_path, colspecs=colspecs, names=colnames, dtype="string")

    label_row = [labels.get(name.split("_")[0], "") for name in colnames]

    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(",".join(colnames) + "\n")
        f.write(",".join(f"\"{lab}\"" for lab in label_row) + "\n")
        df.to_csv(f, index=False, header=False)

    print(f"OK -> {csv_path}  |  lignes={df.shape[0]}, colonnes={df.shape[1]}")

# ---------------------------
# 2) Batch depuis file_list.txt
# ---------------------------

FAM_RE  = re.compile(r'^(FAM\d{4}ER)\.(sas|txt)$', re.IGNORECASE)
WLTH_RE = re.compile(r'^(WLTH\d{4})\.(sas|txt)$', re.IGNORECASE)

def parse_file_list(list_path: Path) -> list[str]:
    lines = [ln.strip() for ln in list_path.read_text(encoding="utf-8").splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

def group_pairs(filenames: list[str]):
    pairs = defaultdict(dict)
    for name in filenames:
        m1 = FAM_RE.match(name)
        m2 = WLTH_RE.match(name)
        if m1:
            base, ext = m1.group(1), m1.group(2).lower()
            pairs[base][ext] = DATA_DIR / name      # ← (CHANGEMENT MINIMAL) joint au dossier des données
        elif m2:
            base, ext = m2.group(1), m2.group(2).lower()
            pairs[base][ext] = DATA_DIR / name
        # sinon on ignore (csv déjà générés, etc.)
    return pairs

def run_batch(skip_existing: bool = True):
    if not LIST_FILE.exists():
        raise FileNotFoundError(f"Liste introuvable : {LIST_FILE}")
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Dossier données introuvable : {DATA_DIR}")

    names = parse_file_list(LIST_FILE)
    pairs = group_pairs(names)

    if not pairs:
        print("Aucun couple (.sas/.txt) détecté dans la liste.")
        return

    for base, exts in sorted(pairs.items()):
        if 'sas' not in exts or 'txt' not in exts:
            print(f"[WARN] Incomplet pour {base} (sas/txt manquant).")
            continue

        layout_path = exts['sas']                  # déjà absolus vers DATA_DIR
        txt_path    = exts['txt']
        out_path    = DATA_DIR / f"{base}_full.csv"

        if skip_existing and out_path.exists():
            print(f"[SKIP] {out_path.name} existe déjà.")
            continue

        try:
            txt_to_csv_full(str(txt_path), str(layout_path), str(out_path))
        except Exception as e:
            print(f"[ERROR] {base}: {e}")

if __name__ == "__main__":
    run_batch(skip_existing=True)
