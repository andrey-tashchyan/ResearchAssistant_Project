# PSID Pipeline - Data Processing and Panel Construction

## 📋 Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Installation](#installation)
5. [Pipeline Overview](#pipeline-overview)
6. [Main Scripts](#main-scripts)
7. [Configuration Files](#configuration-files)
8. [Output Formats](#output-formats)
9. [Usage Examples](#usage-examples)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project implements a **complete pipeline** for processing **Panel Study of Income Dynamics (PSID)** data. It transforms raw SAS/TXT files into structured, exploitable panels.

**Key Features:**
- Memory-efficient processing (handles 10M+ rows)
- Partitioned Parquet output with ZSTD compression
- Automated dependency management
- Detailed logging and progress tracking

**Pipeline Steps:**
1. Convert SAS/TXT → CSV
2. Build variable mapping
3. Create canonical grid (variables × years)
4. Merge similar variables
5. Generate optimized panel output

---

## 🚀 Quick Start

```bash
# 1. Setup environment (one-time)
./setup.sh

# 2. Activate environment
source psid_env/bin/activate

# 3. Run complete pipeline
./run_all.sh

# 4. Access results
ls -lh final_results/
```

**That's it!** The pipeline will process all data and generate output files in `final_results/`.

---

## 📖 User Guide

### First Time Setup (5 minutes)

**Step 1: Prepare your data**
```bash
# Place your PSID files in sorted_data/
# Required files: FAM*.sas, FAM*.txt, WLTH*.sas, WLTH*.txt
cd sorted_data/
ls -lh  # Verify files are present
```

**Step 2: Configure file list**
```bash
# Edit file_list.txt to specify which files to process
nano file_list.txt

# Example content:
# FAM2009ER.sas
# FAM2009ER.txt
# WLTH1999.sas
# WLTH1999.txt
```

**Step 3: Run setup**
```bash
./setup.sh
# Wait ~2 minutes for dependencies to install
```

### Daily Workflow

**Start working:**
```bash
# 1. Navigate to project
cd /path/to/algo3

# 2. Activate environment
source psid_env/bin/activate

# 3. Run pipeline (or individual scripts)
./run_all.sh
```

**Stop working:**
```bash
deactivate  # Exit virtual environment
```

### Common Tasks

**Task 1: Process specific years only**
```bash
# Edit file_list.txt to include only desired years
nano file_list.txt

# Remove unwanted years, keep structure:
# FAM2015ER.sas
# FAM2015ER.txt
# FAM2017ER.sas
# FAM2017ER.txt

# Run pipeline
./run_all.sh
```

**Task 2: Merge custom variables**
```bash
# Edit merge_groups.txt
nano merge_groups.txt

# Add your merge rules (one per line):
# wealth_total home_equity vehicle_equity
# income_labor income_business

# Run merge step
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv < merge_groups.txt
```

**Task 3: Rebuild panel from scratch**
```bash
# Clean previous outputs
rm -rf out/* final_results/*

# Re-run pipeline
./run_all.sh
```

**Task 4: Access data in Python**
```python
import pandas as pd

# Quick load
df = pd.read_parquet('final_results/panel.parquet')
print(df.head())

# Filter by year
df_2009 = pd.read_parquet('final_results/panel.parquet',
                          filters=[('year', '==', 2009)])

# Get specific variables
df_wealth = pd.read_parquet('final_results/panel.parquet',
                            columns=['year', 'family_id', 'ira_balance'])
```

**Task 5: Access data in R**
```r
library(arrow)
library(dplyr)

# Load data
df <- read_parquet("final_results/panel.parquet")

# Filter example
df_filtered <- open_dataset("final_results/panel.parquet") %>%
  filter(year >= 2009) %>%
  select(year, family_id, ira_balance) %>%
  collect()
```

**Task 6: Export to Stata**
```python
import pandas as pd

# Load panel
df = pd.read_csv('final_results/panel_parent_child.csv')

# Export
df.to_stata('my_panel.dta', write_index=False, version=118)
```

### Pipeline Steps Explained

**What each step does:**

| Step | Script | Input | Output | Purpose |
|------|--------|-------|--------|---------|
| 1 | `sas_to_csv.py` | FAM*.sas/txt, WLTH*.sas/txt | *_full.csv | Convert fixed-width to CSV |
| 2 | `create_mapping.py` | *_full.csv | mapping_long.csv | Build variable dictionary |
| 3 | `psid_tool.py` | mapping_long.csv | canonical_grid.csv | Create variables × years grid |
| 4 | `merge_grid.py` | canonical_grid.csv | final_grid.csv | Merge similar variables |
| 5 | `build_final_panel.py` | final_grid.csv + CSV data | panel.parquet/ | Generate optimized panel |

**Typical runtime:**
- Step 1: 5-10 min (depends on file size)
- Step 2: 1-2 min
- Step 3: <1 min
- Step 4: <1 min
- Step 5: 10-30 min (depends on data size)
- **Total: ~20-45 minutes**

### File Organization

**Input structure:**
```
sorted_data/
├── FAM2009ER.sas          # SAS definition
├── FAM2009ER.txt          # Raw data
├── FAM2009ER_full.csv     # Generated CSV (after step 1)
├── WLTH1999.sas
├── WLTH1999.txt
└── WLTH1999_full.csv
```

**Output structure:**
```
out/
├── mapping_long.csv        # Variable dictionary
├── canonical_grid.csv      # Initial grid
└── final_grid.csv          # Grid after merging

final_results/
├── panel.parquet/          # ★ Main output (use this!)
├── panel_grid_by_family.csv
├── panel_parent_child.csv
├── parent_child_links.csv
└── panel_summary.csv
```

### Quick Reference Commands

```bash
# Setup (first time only)
./setup.sh

# Activate environment
source psid_env/bin/activate

# Full pipeline
./run_all.sh

# Individual steps
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
python create_mapping.py --data-dir sorted_data --out-dir out
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv < merge_groups.txt
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results

# Check outputs
ls -lh final_results/
du -sh final_results/panel.parquet/

# Deactivate
deactivate
```

### Tips & Best Practices

**✓ Do's:**
- Always activate environment before running scripts
- Check logs for errors during processing
- Use Parquet format for large datasets (faster, smaller)
- Keep original data files backed up
- Version control your configuration files (file_list.txt, merge_groups.txt)

**✗ Don'ts:**
- Don't edit generated files in `out/` or `final_results/` manually
- Don't run scripts without activating environment
- Don't delete `sorted_data/` after processing (needed for rebuilds)
- Don't commit data files to git repositories
- Don't process all years at once if low on RAM (split by year)

### Monitoring Progress

**Check pipeline status:**
```bash
# Watch real-time output
./run_all.sh | tee pipeline.log

# Check if step completed
ls -lh out/final_grid.csv
ls -lh final_results/panel.parquet/

# View last N lines of log
tail -100 pipeline.log
```

**Verify data quality:**
```python
import pandas as pd

# Check panel summary
summary = pd.read_csv('final_results/panel_summary.csv')
print(summary)

# Check number of observations
df = pd.read_parquet('final_results/panel.parquet')
print(f"Total observations: {len(df):,}")
print(f"Families: {df['family_id'].nunique():,}")
print(f"Years: {sorted(df['year'].unique())}")
```

---

## 📁 Project Structure

```
algo3/
├── sorted_data/              # Raw PSID files (FAM*, WLTH*)
├── out/                      # Intermediate results
│   ├── mapping_long.csv      # Variable dictionary
│   ├── canonical_grid.csv    # Variables × years grid
│   └── final_grid.csv        # Final grid after merging
├── final_results/            # Final outputs
│   ├── panel.parquet/        # Optimized panel (recommended)
│   ├── panel_grid_by_family.csv
│   └── panel_parent_child.csv
├── setup.sh                  # Environment setup (run once)
├── run_all.sh                # Master pipeline script
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

---

## 💻 Installation

### Requirements
- Python 3.8+
- 8 GB RAM minimum (16 GB recommended)
- 10 GB disk space

### Setup

```bash
# Automatic setup (recommended)
./setup.sh

# Manual setup
python3 -m venv psid_env
source psid_env/bin/activate
pip install -r requirements.txt
```

**Dependencies:**
- pandas, numpy, pyarrow (core data processing)
- tqdm (progress bars)
- openpyxl (Excel support)
- regex, multiprocess (utilities)

---

## 🔄 Pipeline Overview

The pipeline runs in **5 sequential steps**:

```
[1] SAS/TXT → CSV  →  [2] Mapping  →  [3] Grid  →  [4] Merge  →  [5] Panel
```

### Step-by-step Execution

```bash
# Complete pipeline (recommended)
./run_all.sh

# Or run individual steps:
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
python create_mapping.py --data-dir sorted_data --out-dir out
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv < merge_groups.txt
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results
```

---

## 📝 Main Scripts

### 1. sas_to_csv.py
Converts raw PSID files (fixed-width TXT) to CSV.

```bash
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
```

### 2. create_mapping.py
Builds variable dictionary with canonical names.

```bash
python create_mapping.py --data-dir sorted_data --out-dir out
```

**Output:** `mapping_long.csv` with columns: canonical, year, var_code, label, category, dtype

### 3. psid_tool.py
Creates canonical grid (variables × years matrix).

```bash
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH
```

**Output:** `canonical_grid.csv` with format:
```csv
row,concept,required,1999,2001,2003,...,2023
1,family_id,1,ER30001,ER31001,ER32001,...
2,ira_balance,0,S517,S617,S717,...
```

### 4. merge_grid.py
Merges similar variables based on rules in `merge_groups.txt`.

```bash
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv < merge_groups.txt
```

### 5. build_final_panel.py
Generates optimized Parquet panel.

```bash
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results --rebuild
```

**Output:** Partitioned `panel.parquet/` directory

### 6. build_panel_parent_child.py (Optional)
Creates family-focused panel with parent-child relationships.

```bash
python build_panel_parent_child.py \
  --final-grid out/final_grid.csv \
  --mapping out/mapping_long.csv \
  --data-dir sorted_data \
  --out-dir final_results
```

**Outputs:**
- `panel_parent_child.csv` - Long panel
- `panel_grid_by_family.csv` - Wide format per family
- `parent_child_links.csv` - Relationships
- `panel_summary.csv` - Statistics

---

## 📄 Configuration Files

### file_list.txt
List of SAS/TXT pairs to convert:
```
FAM2009ER.sas
FAM2009ER.txt
WLTH1999.sas
WLTH1999.txt
...
```

### merge_groups.txt
Variable merge rules (one group per line):
```
ira_balance ira_any ira_num
wealth_wo_equity home_equity
vehicles vehicle
```

**Logic:** For each year, takes first non-empty value from left to right.

---

## 📤 Output Formats

### 1. Parquet Panel (Recommended)

**File:** `final_results/panel.parquet/`

**Reading in Python:**
```python
import pandas as pd

# Read complete panel
df = pd.read_parquet('final_results/panel.parquet')

# Read specific year (fast!)
df_2009 = pd.read_parquet('final_results/panel.parquet',
                          filters=[('year', '==', 2009)])

# Read with column selection
df = pd.read_parquet('final_results/panel.parquet',
                     columns=['year', 'family_id', 'ira_balance'])
```

**Reading in R:**
```r
library(arrow)
df <- read_parquet("final_results/panel.parquet")

# With filters
df_2009 <- open_dataset("final_results/panel.parquet") %>%
  filter(year == 2009) %>%
  collect()
```

### 2. Panel by Family (Wide Format)

**File:** `final_results/panel_grid_by_family.csv`

Structure:
- Rows: (family_id, concept)
- Columns: years (1999, 2001, 2003, ...)
- Ideal for longitudinal family analysis

### 3. Long Panel

**File:** `final_results/panel_parent_child.csv`

Classic panel data format:
- Columns: year, family_id, person_id, mother_id, father_id, variables...
- One row per person-year observation

---

## 💡 Usage Examples

### Example 1: Basic Analysis

```python
import pandas as pd

# Load data
df = pd.read_parquet('final_results/panel.parquet')

# Summary statistics by year
summary = df.groupby('year').agg({
    'ira_balance': ['mean', 'median', 'std'],
    'num_children': 'mean'
})
print(summary)
```

### Example 2: Wealth Evolution

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load WLTH module only
df = pd.read_parquet('final_results/panel.parquet',
                     filters=[('module', '==', 'WLTH')])

# Calculate average by year
wealth = df.groupby('year')['ira_balance'].mean()

# Plot
wealth.plot(kind='line', marker='o')
plt.title('Average IRA Balance Over Time')
plt.xlabel('Year')
plt.ylabel('Balance ($)')
plt.savefig('wealth_trend.png')
```

### Example 3: Family-Level Analysis

```python
import pandas as pd

# Load family panel
panel = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extract one family
family = panel[panel['family_id'] == '100001']
family_pivot = family.set_index('concept').drop(columns='family_id')

# View evolution
print(family_pivot.T)  # Transpose to see years as rows
```

### Example 4: Export for Stata

```python
import pandas as pd

df = pd.read_csv('final_results/panel_parent_child.csv')
df.to_stata('panel.dta', write_index=False, version=118)
```

---

## 🛠️ Troubleshooting

### Common Issues

**1. "No module named 'pyarrow'"**
```bash
pip install pyarrow
```

**2. MemoryError**
- Reduce number of years in `file_list.txt`
- Increase system RAM or swap
- Process years individually

**3. "FileNotFoundError: final_grid.csv"**
```bash
# Re-run pipeline from start
./run_all.sh
```

**4. Empty columns in output**
- Check that variables exist in source files
- Verify `mapping_long.csv` contains the variables

**5. Very slow execution**
```bash
# Install progress bars
pip install tqdm

# Or process FAM/WLTH separately in parallel
```

**6. "Permission denied: run_all.sh"**
```bash
chmod +x run_all.sh setup.sh
./run_all.sh
```

### Getting Help

1. Check error messages in terminal output
2. Review `out/codes_resolved_audit.csv` for mapping issues
3. Verify input files in `sorted_data/` are complete
4. Consult script docstrings: `python script.py --help`

---

## ⚡ Performance Tips

### Memory Optimization
The pipeline automatically:
- Downcasts int64 → Int32 (50% reduction)
- Converts repetitive strings → category (80% reduction)
- Processes data in chunks
- Uses copy-on-write mode

**Typical memory usage:**
- Raw: 8 GB → Optimized: 1.5 GB → Parquet: 400 MB

### Speed Optimization
- Use SSD if possible
- Install `tqdm` for progress tracking
- Process years in parallel (manual)
- Use `--rebuild` only when needed

---

## 📚 Additional Resources

- **PSID Official Site:** https://psidonline.isr.umich.edu/
- **Variable Documentation:** https://simba.isr.umich.edu/
- **User Guide:** https://psidonline.isr.umich.edu/Guide/

---

## 🔐 Data Privacy

**Important:** PSID data may contain sensitive information.

```bash
# Add to .gitignore
sorted_data/
out/
final_results/
*.csv
*.parquet

# Secure file permissions
chmod 600 final_results/*.csv
chmod 700 final_results/panel.parquet/
```

---

## 📝 Summary

**Setup:**
```bash
./setup.sh && source psid_env/bin/activate
```

**Run:**
```bash
./run_all.sh
```

**Read data:**
```python
import pandas as pd
df = pd.read_parquet('final_results/panel.parquet')
```

**Activate later:**
```bash
source psid_env/bin/activate
```

---

**Version:** 3.0
**Last Updated:** 2024-11-01
**Authors:** PSID Pipeline RA Team
