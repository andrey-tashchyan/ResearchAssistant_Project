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

### What is this project?

This project is a **data processing pipeline** designed specifically for researchers working with **Panel Study of Income Dynamics (PSID)** data. PSID is one of the longest-running household surveys in the world, tracking families and individuals over time (since 1968).

**The Problem:** PSID data comes in a complex format (SAS files with fixed-width text) that's difficult to work with directly. Variables have cryptic names (like "ER30001" instead of "family_id"), and the structure changes across years.

**Our Solution:** This pipeline automatically:
- Converts cryptic PSID files into clean, readable formats
- Standardizes variable names across all years
- Organizes everything into an easy-to-analyze panel dataset
- Optimizes file sizes for faster loading (from GB to MB)

### Key Features

- **Memory-efficient**: Can handle datasets with 10M+ rows without crashing
- **Fast**: Optimized Parquet format loads 10-50x faster than CSV
- **Automated**: One command runs the entire pipeline
- **Well-documented**: Detailed logs show exactly what's happening
- **Cross-platform**: Works on Windows, macOS, and Linux

### How it works (simplified)

```
Raw PSID files          →  Readable CSVs       →  Standardized panel
(FAM2009ER.txt)            (family data 2009)     (ready for analysis)
cryptic codes              human-readable         all years aligned
```

The pipeline does 5 main things:
1. **Converts** SAS/TXT files to readable CSV format
2. **Builds** a dictionary translating codes to meaningful names
3. **Creates** a grid showing which variables exist in which years
4. **Merges** similar variables that were named differently
5. **Generates** the final optimized dataset ready for analysis

---

## 🚀 Quick Start

**Unix/macOS/Linux:**
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

**Windows (PowerShell):**
```powershell
# 1. Setup environment (one-time)
.\setup.ps1

# 2. Activate environment
.\psid_env\Scripts\Activate.ps1

# 3. Run individual scripts (no run_all.sh on Windows)
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
python create_mapping.py --data-dir sorted_data --out-dir out
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv
# (merge_groups.txt must be piped manually or use Get-Content merge_groups.txt | python merge_grid.py ...)
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results

# 4. Access results
dir final_results
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

Unix/macOS/Linux:
```bash
./setup.sh
# Wait ~2 minutes for dependencies to install
```

Windows (PowerShell):
```powershell
.\setup.ps1
# Wait ~2 minutes for dependencies to install
```

### Daily Workflow

**Unix/macOS/Linux:**
```bash
# 1. Navigate to project
cd /path/to/algo3

# 2. Activate environment
source psid_env/bin/activate

# 3. Run pipeline (or individual scripts)
./run_all.sh

# 4. Stop working
deactivate
```

**Windows (PowerShell):**
```powershell
# 1. Navigate to project
cd C:\path\to\algo3

# 2. Activate environment
.\psid_env\Scripts\Activate.ps1

# 3. Run individual scripts (see Pipeline Overview section)
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
# ... (other scripts)

# 4. Stop working
deactivate
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
- ~6 GB RAM minimum
- 1 GB disk space

### Setup

**Unix/macOS/Linux:**
```bash
# Automatic setup (recommended)
./setup.sh

# Manual setup
python3 -m venv psid_env
source psid_env/bin/activate
pip install -r requirements.txt
```

**Windows:**
```powershell
# Automatic setup (recommended)
.\setup.ps1

# If you get an execution policy error, run first:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Manual setup
python -m venv psid_env
.\psid_env\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Dependencies:**
- pandas, numpy, pyarrow (core data processing)
- tqdm (progress bars)
- openpyxl (Excel support)
- regex, multiprocess (utilities)

**Windows Users:** See [WINDOWS.md](WINDOWS.md) for detailed Windows-specific instructions, troubleshooting, and alternatives (WSL, Git Bash).

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

## 📝 Main Scripts Explained

Each script performs a specific task in the pipeline. Here's what each one does and why:

### 1. sas_to_csv.py - File Conversion

**What it does:** Converts PSID's raw fixed-width text files into standard CSV format.

**Why it matters:** PSID data comes as `.txt` files where each variable occupies fixed character positions (like columns 1-5 = family ID, columns 6-8 = age, etc.). This is hard to work with. This script reads the `.sas` definition files to understand the structure, then properly extracts each variable into a CSV where each column is clearly labeled.

**Example:**
```bash
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
```

**What happens:**
- Reads `FAM2009ER.sas` to learn column positions
- Reads `FAM2009ER.txt` and extracts data based on those positions
- Creates `FAM2009ER_full.csv` with proper column headers
- Adds a second row with human-readable labels

**Time:** ~2 minutes per file

---

### 2. create_mapping.py - Build Variable Dictionary

**What it does:** Creates a master dictionary that translates PSID codes into meaningful variable names.

**Why it matters:** PSID uses cryptic codes like "ER42001" for family_id, "S517" for IRA balance. These codes change every year! This script scans all your CSV files, extracts all variable names and their descriptions, then standardizes them into consistent "canonical" names.

**Example:**
```bash
python create_mapping.py --data-dir sorted_data --out-dir out
```

**What happens:**
- Scans all `*_full.csv` files in `sorted_data/`
- Extracts variable codes and their labels
- Normalizes names (e.g., "ira balance", "IRA Balance", "ira_balance" all become "ira_balance")
- Creates `mapping_long.csv` showing: canonical_name, year, original_code, description

**Output example:**
```csv
canonical,year,var_code,label,category
family_id,2009,ER42001,"Family Interview Number",Demographics
family_id,2011,ER47301,"Family Interview Number",Demographics
ira_balance,1999,S517,"IRA Account Balance",Wealth
```

**Time:** ~1 minute

---

### 3. psid_tool.py - Create Variables × Years Grid

**What it does:** Reorganizes the mapping into a spreadsheet-like grid showing which PSID code corresponds to each concept in each year.

**Why it matters:** Instead of having thousands of rows (one per variable-year combination), you get a compact grid where each row is a concept and each column is a year. This makes it easy to see coverage and missing data.

**Example:**
```bash
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH
```

**Parameters explained:**
- `--mapping`: Uses the dictionary we just created
- `--prefer WLTH`: When a variable appears in both FAM and WLTH modules, prefer WLTH version

**Output (`canonical_grid.csv`):**
```csv
row,concept,required,1999,2001,2003,2009,2011
1,family_id,1,ER30001,ER31001,ER32001,ER42001,ER47301
2,ira_balance,0,S517,S617,S717,ER46946,ER52316
3,num_children,1,,,ER33404,ER42003,ER47303
```

Each cell shows the PSID code for that concept in that year (empty = not available).

**Time:** <30 seconds

---

### 4. merge_grid.py - Combine Similar Variables

**What it does:** Merges rows that represent the same concept but were captured slightly differently.

**Why it matters:** Sometimes PSID asks the same thing in different ways: "Do you have IRA?" (yes/no) vs "IRA balance" (dollar amount). You want to combine these into one variable. This script does that based on rules you define.

**Example:**
```bash
python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv < merge_groups.txt
```

**How it works:**
1. Reads `merge_groups.txt` which lists variables to merge:
   ```
   ira_balance ira_any ira_num
   wealth_wo_equity home_equity
   ```
2. For each group, creates a single merged row
3. For each year, takes the first non-empty value from left to right
4. Output saved as `final_grid.csv`

**Example merge:**
```
Before:
  ira_balance: [S517, "", S717]
  ira_any:     ["", S618, ""]

After:
  ira_balance_merged: [S517, S618, S717]  ← filled the gap!
```

**Time:** <10 seconds

---

### 5. build_final_panel.py - Generate Analysis-Ready Dataset

**What it does:** Creates the final panel dataset by reading all CSV files, extracting the variables specified in `final_grid.csv`, and saving everything in an optimized format.

**Why it matters:** This is where all the pieces come together. The script:
- Reads your actual data from `sorted_data/*_full.csv`
- Extracts only the variables you need (not all 700+ variables!)
- Renames cryptic codes to readable names
- Optimizes data types to save memory
- Saves in Parquet format (super fast to load, 70% smaller than CSV)

**Example:**
```bash
python build_final_panel.py \
  --data-dir sorted_data \
  --out-dir out \
  --final-dir final_results \
  --rebuild
```

**Parameters explained:**
- `--data-dir`: Where your CSV files are
- `--out-dir`: Where to find `final_grid.csv` and `mapping_long.csv`
- `--final-dir`: Where to save the final panel
- `--rebuild`: Delete and recreate (optional, use if you made changes)

**What happens:**
1. Reads `final_grid.csv` to know what to extract
2. For each year-module (e.g., FAM2009, WLTH1999):
   - Opens the CSV
   - Extracts only needed columns
   - Renames them to canonical names
   - Optimizes data types (int64→int32, string→category where appropriate)
   - Saves as compressed Parquet partition
3. Creates a partitioned directory structure for fast filtered queries

**Output structure:**
```
panel.parquet/
├── year=1999/
│   ├── module=FAM/part-0.parquet
│   └── module=WLTH/part-0.parquet
├── year=2001/
│   └── ...
```

**Time:** 10-30 minutes (depends on data size)

**Memory optimizations:**
- Processes one chunk at a time (doesn't load everything)
- Downcasts integers (saves 50% memory)
- Converts repetitive strings to categories (saves 80%)
- Result: 8GB data → 1.5GB in memory → 400MB on disk

---

### 6. build_panel_parent_child.py - Family Relationships (Optional)

**What it does:** Creates a specialized version of the panel focused on parent-child relationships within families.

**Why it matters:** If you're studying intergenerational dynamics, you need to identify who is a parent and who is a child, then track families over time. This script:
- Identifies parents using mother_id/father_id fields
- Filters to families with children
- Creates multiple views: long panel, wide panel, relationship links

**Example:**
```bash
python build_panel_parent_child.py \
  --final-grid out/final_grid.csv \
  --mapping out/mapping_long.csv \
  --data-dir sorted_data \
  --out-dir final_results
```

**Outputs:**

1. **panel_parent_child.csv** - Traditional long format
   - One row per person-year
   - Columns: year, family_id, person_id, mother_id, father_id, [all variables]

2. **panel_grid_by_family.csv** - Wide format by family
   - Rows: (family_id, concept) pairs
   - Columns: years
   - Easy to see one family's evolution over time

3. **parent_child_links.csv** - Relationship mapping
   - Shows who is a parent and who they're connected to

4. **panel_summary.csv** - Quick statistics
   - Non-missing counts, means, medians for each variable

**Time:** 5-15 minutes

---

## 📄 Configuration Files

These simple text files control which data gets processed and how variables are combined.

### file_list.txt - What files to process

**Purpose:** Tells the pipeline which PSID files to convert from SAS/TXT to CSV.

**Format:** List SAS and TXT files in pairs (one per line):
```
FAM2009ER.sas
FAM2009ER.txt
FAM2011ER.sas
FAM2011ER.txt
WLTH1999.sas
WLTH1999.txt
```

**Why pairs?** The `.sas` file contains the "recipe" (column positions and widths), while the `.txt` file contains the actual data. You need both.

**How to edit:**
- Open in any text editor (Notepad, VS Code, nano)
- Add/remove file pairs as needed
- Keep them in pairs (SAS first, then TXT)
- Don't include the full path, just the filename (files should be in `sorted_data/`)

**Example - Processing only 2 years:**
```
FAM2015ER.sas
FAM2015ER.txt
FAM2017ER.sas
FAM2017ER.txt
```

---

### merge_groups.txt - How to combine variables

**Purpose:** Defines which similar variables should be merged into a single concept.

**Format:** One group per line, space-separated variable names:
```
ira_balance ira_any ira_num
wealth_wo_equity home_equity
vehicles vehicle
```

**Why merge?** PSID sometimes captures the same concept different ways:
- Year 1999: "Do you have IRA?" (yes/no)
- Year 2001: "IRA balance" (dollar amount)
- Year 2003: "Number of IRAs" (count)

You want these as ONE variable called "ira_balance" that uses whichever is available.

**Logic:** For each year, takes the first non-empty value from left to right.

**Example:**
```
Before merging:
  ira_balance: [1999: $5000,  2001: empty,   2003: $8000]
  ira_any:     [1999: empty,  2001: "yes",   2003: empty]
  ira_num:     [1999: empty,  2001: empty,   2003: 2]

After merging "ira_balance ira_any ira_num":
  ira_balance_merged: [1999: $5000, 2001: "yes", 2003: $8000]
                              ↑          ↑            ↑
                          from left   from middle  from left
```

**How to edit:**
1. Open in text editor
2. Each line is one merge group
3. Put preferred variable first (will be used as base name)
4. Add more variables to the right as fallbacks
5. Save the file

**Common patterns:**
```
# Merge wealth components
total_wealth net_worth wealth_wo_equity

# Merge employment status variants
employed working has_job

# Merge income sources
labor_income wage_income employment_income
```

---

## 📤 Output Formats Explained

The pipeline generates multiple output formats for different use cases. Here's what each one is and when to use it:

---

### 1. Parquet Panel (Recommended for most analyses)

**File:** `final_results/panel.parquet/` (directory)

**What is it?** A highly optimized, compressed format designed for fast data analysis. Think of it as "CSV on steroids" - same data structure, but loads 10-50x faster and takes 70% less disk space.

**Why use it?**
- **Fast**: Loading years of data takes seconds instead of minutes
- **Efficient**: Compressed with ZSTD (similar quality to zipped CSV)
- **Filtered queries**: Can load only specific years/columns without reading everything
- **Type-safe**: Preserves data types (no accidental string-to-number conversions)

**Structure:**
- Partitioned by year (and optionally module)
- Each partition is a separate compressed file
- Metadata allows smart filtering

**When to use:**
- Any analysis in Python or R
- When working with large datasets
- When you need to repeatedly load subsets of data

**Reading in Python:**
```python
import pandas as pd

# Example 1: Read everything (be careful with memory!)
df = pd.read_parquet('final_results/panel.parquet')
print(df.shape)  # Shows dimensions: (rows, columns)

# Example 2: Read specific year only (FAST!)
df_2009 = pd.read_parquet('final_results/panel.parquet',
                          filters=[('year', '==', 2009)])
# Only loads 2009 data, ignores other years

# Example 3: Read multiple years
df_recent = pd.read_parquet('final_results/panel.parquet',
                            filters=[('year', 'in', [2015, 2017, 2019])])

# Example 4: Read specific columns only (saves memory)
df_wealth = pd.read_parquet('final_results/panel.parquet',
                            columns=['year', 'family_id', 'ira_balance', 'net_worth'])
# Only loads these 4 columns, ignores the rest

# Example 5: Combined filtering
df_subset = pd.read_parquet('final_results/panel.parquet',
                            filters=[('year', '>=', 2009), ('module', '==', 'WLTH')],
                            columns=['year', 'family_id', 'ira_balance'])
# Loads only WLTH module, years 2009+, 3 columns
```

**Reading in R:**
```r
library(arrow)
library(dplyr)

# Example 1: Read everything
df <- read_parquet("final_results/panel.parquet")

# Example 2: Read with filters (recommended)
df_2009 <- open_dataset("final_results/panel.parquet") %>%
  filter(year == 2009) %>%
  collect()

# Example 3: More complex query
df_wealth <- open_dataset("final_results/panel.parquet") %>%
  filter(year >= 2009, module == "WLTH") %>%
  select(year, family_id, ira_balance) %>%
  collect()
```

**File size comparison:**
- Original CSV: 8 GB
- Parquet: ~500 MB (85% smaller!)

---

### 2. Panel by Family - Wide Format (For tracking individual families)

**File:** `final_results/panel_grid_by_family.csv`

**What is it?** Each family gets multiple rows (one per variable), with columns showing values across years.

**Structure:**
```csv
family_id,concept,1999,2001,2003,2005,2009,2011
100001,family_id,100001,100001,100001,100001,100001,100001
100001,num_children,2,2,3,3,2,2
100001,ira_balance,15000,18000,22000,28000,35000,42000
100001,income,45000,48000,52000,58000,65000,71000
100002,family_id,100002,100002,100002,100002,100002,100002
100002,num_children,1,1,1,2,2,2
...
```

**When to use:**
- You want to track one family's evolution over time
- Creating family-level visualizations (line charts showing one family)
- Calculating family-specific growth rates
- Identifying transitions (e.g., when did family size change?)

**How to read:**
```python
import pandas as pd

# Load the file
panel = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extract one family
family_100001 = panel[panel['family_id'] == '100001']

# Reshape for easier viewing (concepts as rows, years as columns)
family_pivot = family_100001.set_index('concept').drop(columns='family_id')
print(family_pivot)

# Calculate growth rate for one family
ira_values = family_pivot.loc['ira_balance'].astype(float)
ira_growth = ira_values.pct_change() * 100  # Percentage change
print(f"IRA growth rates: {ira_growth}")
```

**Advantages:**
- Easy to see one family's complete history
- Natural format for time series analysis of individuals
- Can spot missing data patterns quickly

**Disadvantages:**
- Large file (one row per family-concept pair)
- Not ideal for cross-sectional analysis

---

### 3. Long Panel - Traditional Format (For econometric analysis)

**File:** `final_results/panel_parent_child.csv`

**What is it?** Classic panel data structure - one row per observation (person-year combination).

**Structure:**
```csv
year,family_id,person_id,mother_id,father_id,ira_balance,income,num_children
1999,100001,101,102,103,15000,45000,2
1999,100001,102,,,0,32000,
1999,100001,103,,,0,38000,
2001,100001,101,102,103,18000,48000,2
2001,100001,102,,,5000,35000,
...
```

**When to use:**
- Regression analysis (panel regressions, fixed effects)
- Standard econometric models
- Any analysis that expects "long" format
- Exporting to Stata, SAS, or SPSS

**How to read:**
```python
import pandas as pd

# Load the file
df = pd.read_csv('final_results/panel_parent_child.csv')

# Basic exploration
print(f"Total observations: {len(df):,}")
print(f"Unique families: {df['family_id'].nunique():,}")
print(f"Unique persons: {df['person_id'].nunique():,}")
print(f"Years covered: {sorted(df['year'].unique())}")

# Example analysis: average by year
yearly_avg = df.groupby('year')['ira_balance'].mean()
print(yearly_avg)

# Example: filter to parents only
parents = df[
    df['person_id'].isin(df['mother_id']) |
    df['person_id'].isin(df['father_id'])
]
print(f"Parent observations: {len(parents):,}")
```

**Advantages:**
- Standard format for panel econometrics
- Compatible with most statistical software
- Easy to add person-level or family-level variables
- Natural for regression models

**Disadvantages:**
- Can be very large (millions of rows)
- Requires more memory to load completely

---

### Format Comparison Table

| Format | Best For | File Size | Load Speed | Use Case |
|--------|----------|-----------|------------|----------|
| **Parquet** | Most analyses | Smallest | Fastest | General purpose, Python/R |
| **Wide (by family)** | Family tracking | Large | Medium | Individual family evolution |
| **Long** | Econometrics | Largest | Slowest | Regressions, Stata export |

---

### Which format should I use?

**Start with Parquet if:**
- You're working in Python or R
- Your dataset is large (1M+ rows)
- You need to load subsets frequently
- You want the fastest analysis workflow

**Use Wide format if:**
- You're studying specific families over time
- You want to visualize individual trajectories
- You need to calculate family-level statistics

**Use Long format if:**
- You're running panel regressions
- You need to export to Stata/SAS
- Your analysis expects traditional panel structure
- You're following econometric textbook examples

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

