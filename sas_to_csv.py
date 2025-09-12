import re
import pandas as pd
from collections import Counter
from pathlib import Path

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

    # construire la ligne de labels (2e ligne du CSV)
    label_row = [labels.get(name.split("_")[0], "") for name in colnames]

    # écrire le CSV : d’abord les noms techniques, ensuite les labels, ensuite les données
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(",".join(colnames) + "\n")
        f.write(",".join(f"\"{lab}\"" for lab in label_row) + "\n")
        df.to_csv(f, index=False, header=False)

    print(f"OK -> {csv_path}  |  lignes={df.shape[0]}, colonnes={df.shape[1]}")