# PSID Pipeline - Data Processing and Panel Construction

## 📋 Table of Contents

1. [Overview](#overview)
2. [Project Architecture](#project-architecture)
3. [Installation and Prerequisites](#installation-and-prerequisites)
4. [Data Structure](#data-structure)
5. [Complete Pipeline](#complete-pipeline)
6. [Detailed Scripts](#detailed-scripts)
7. [Configuration Files](#configuration-files)
8. [Usage](#usage)
9. [Output Formats](#output-formats)
10. [Memory Optimizations](#memory-optimizations)
11. [Troubleshooting](#troubleshooting)
12. [Usage Examples](#usage-examples)

---

## 🎯 Overview

This project implements a **complete and optimized pipeline** for processing **Panel Study of Income Dynamics (PSID)** data. It transforms raw SAS/TXT files into structured, exploitable panels with a particular focus on:

- **Memory efficiency**: Handling datasets with tens of millions of rows
- **Traceability**: Detailed logging of each step
- **Flexibility**: Configuration via external files
- **Performance**: Partitioned Parquet output with ZSTD compression

### Main Objectives

1. **Conversion**: Transform SAS/TXT files into exploitable CSV
2. **Mapping**: Create canonical correspondence between variables and years
3. **Canonical Grid**: Build a variables × years matrix
4. **Merging**: Combine similar variables according to defined rules
5. **Final Panel**: Generate memory-optimized long panels by family and year

---

## 🏗️ Project Architecture

```
algo3/
│
├── 📂 sorted_data/              # PSID source data
│   ├── FAM2009ER.sas           # SAS definition scripts
│   ├── FAM2009ER.txt           # Raw data (fixed-width format)
│   ├── FAM2009ER_full.csv      # Converted CSV (generated)
│   ├── WLTH1999.sas
│   ├── WLTH1999.txt
│   └── ...                      # Other years (1999-2023)
│
├── 📂 out/                      # Intermediate results
│   ├── mapping_long.csv         # Complete variable dictionary
│   ├── fam_wlth_inventory.csv  # FAM/WLTH modules inventory
│   ├── canonical_grid.csv       # Initial canonical grid
│   ├── canonical_grid_merged.csv # Grid after row merging
│   └── final_grid.csv           # Final grid used for extraction
│
├── 📂 final_results/            # Final results
│   ├── panel_parent_child.csv   # Individual long panel
│   ├── parent_child_links.csv   # Parent-child relationships
│   ├── panel_summary.csv        # Descriptive statistics
│   ├── codes_resolved_audit.csv # Correspondence log
│   ├── panel_grid_by_family.csv # ★ Panel by family (wide format)
│   ├── panel.parquet/           # ★ Optimized panel (partitioned format)
│   └── family_grids/            # (Optional) One CSV per family
│
├── 📜 Main Python scripts
│   ├── sas_to_csv.py            # [1] SAS/TXT → CSV conversion
│   ├── create_mapping.py        # [2] Mapping construction
│   ├── psid_tool.py             # [3] Canonical grid
│   ├── merge_grid.py            # [4] Row merging
│   ├── build_final_panel.py     # [5] Optimized Parquet panel
│   └── build_panel_parent_child.py # Family-child panel
│
├── 📜 Utility scripts
│   ├── filter_grid_rows.py      # Grid filtering
│   ├── make_canonical_grid.py   # Alternative for canonical grid
│   ├── no_children_from_gid.py  # GID analysis without children
│   ├── sas_to_csv_gid.py        # GID-specific conversion
│   └── build_parent_child_presence_matrix.py # Presence matrix
│
├── 📜 Configuration files
│   ├── file_list.txt            # List of files to convert
│   └── merge_groups.txt         # Variable merging rules
│
├── 📜 Orchestration
│   ├── run_all.sh               # ★ Master script executing entire pipeline
│   └── README.md                # This file
│
└── 📂 .vscode/                  # VSCode configuration
    └── extensions.json
```

---

## 💻 Installation and Prerequisites

### System Requirements

- **Python 3.8+** (tested with 3.9 and 3.10)
- **8 GB RAM minimum** (16 GB recommended for large datasets)
- **10 GB disk space** for intermediate and final files

### Python Dependencies

```bash
# Main dependencies
pip install pandas>=1.5.0
pip install numpy>=1.23.0
pip install pyarrow>=10.0.0  # For Parquet format

# Optional but recommended
pip install tqdm              # Progress bars
pip install fastparquet       # Alternative to pyarrow
```

### Complete Installation with Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install pandas numpy pyarrow tqdm

# Verify installation
python -c "import pandas; import pyarrow; print('OK')"
```

---

## 📊 Data Structure

### PSID Sources

The project processes two main types of PSID modules:

#### 1. **FAM (Family) Module** - Demographic and family data
- Family variables (household size, composition)
- Head of household characteristics
- Information about children
- Format: `FAM{YEAR}ER.txt` and `FAM{YEAR}ER.sas`
- Available years: 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023

#### 2. **WLTH (Wealth) Module** - Asset data
- Assets (real estate, stocks, savings)
- Debts (mortgages, loans)
- IRAs and retirement accounts
- Format: `WLTH{YEAR}.txt` and `WLTH{YEAR}.sas`
- Available years: 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, ...

### Source File Format

**SAS Files (.sas)**
- Reading scripts defining column positions and widths
- Contain metadata (variable names, types, labels)

**TXT Files (.txt)**
- Raw data in fixed-width format
- No separators, positions defined by .sas file
- Example: one line = one record, each variable at fixed position

---

## 🔄 Complete Pipeline

The pipeline runs in **5 sequential steps** via the `run_all.sh` script:

```
[1] SAS/TXT → CSV  →  [2] Mapping  →  [3] Canonical Grid  →  [4] Merging  →  [5] Final Panel
```

### Flow Diagram

```
┌─────────────────┐
│  file_list.txt  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│ [1] sas_to_csv.py               │
│ Input:  FAM*.sas, FAM*.txt      │
│         WLTH*.sas, WLTH*.txt    │
│ Output: *_full.csv              │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ [2] create_mapping.py           │
│ Input:  sorted_data/*.csv       │
│ Output: mapping_long.csv        │
│         fam_wlth_inventory.csv  │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ [3] psid_tool.py                │
│ Input:  mapping_long.csv        │
│ Output: canonical_grid.csv      │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ [4] merge_grid.py               │
│ Input:  canonical_grid.csv      │
│         merge_groups.txt        │
│ Output: canonical_grid_merged   │
│         → final_grid.csv        │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ [5] build_final_panel.py        │
│ Input:  final_grid.csv          │
│         mapping_long.csv        │
│         sorted_data/*.csv       │
│ Output: panel.parquet/          │
└─────────────────────────────────┘
```

---

## 📝 Detailed Scripts

### 1️⃣ sas_to_csv.py - SAS/TXT to CSV Conversion

**Objective**: Convert raw PSID files (fixed-width TXT) into exploitable CSV.

#### Features
- Parse SAS scripts (.sas) to extract column metadata
- Read TXT files with `pd.read_fwf()` (fixed-width format)
- Add label row (2nd line of CSV)
- Error handling with fallback
- Progress bars (if tqdm installed)

#### Usage
```bash
# Manual mode
python sas_to_csv.py \
  --file-list file_list.txt \
  --out-dir sorted_data

# Via run_all.sh
./run_all.sh  # Step 1 automatic
```

#### Inputs
- `file_list.txt`: List of files to convert
  ```
  FAM2009ER.sas
  FAM2009ER.txt
  WLTH1999.sas
  WLTH1999.txt
  ```

#### Outputs
- `sorted_data/FAM{YEAR}ER_full.csv`
- `sorted_data/WLTH{YEAR}_full.csv`
- Structure:
  - Line 1: Variable names (codes)
  - Line 2: Descriptive labels
  - Following lines: Data

#### Performance
- ~1-2 minutes per file (varies by size)
- Colorized logging with execution time

---

### 2️⃣ create_mapping.py - Variable Dictionary Construction

**Objective**: Create comprehensive correspondence table between PSID variables and their definitions.

#### Features
- Analyzes headers of all generated CSV files
- Extracts labels from 2nd line
- Normalizes variable names into canonical concepts
- Automatically detects type (FAM/WLTH)
- Applies synonym and normalization rules
- Guesses data types (numeric/string)

#### Usage
```bash
python create_mapping.py \
  --data-dir sorted_data \
  --out-dir out
```

#### Main Outputs

**mapping_long.csv** - Comprehensive long format
```csv
canonical,year,file_type,var_code,label,category,dtype,required,transform
family_id,2009,FAM,ER42001,"Family ID",Demographics,string,1,
num_children,2009,FAM,ER42003,"Number of children",Demographics,int,1,
ira_balance,1999,WLTH,S517,"IRA Balance",Retirement/IRA,float,0,
```

Columns:
- `canonical`: Normalized concept name
- `year`: Survey year
- `file_type`: FAM or WLTH
- `var_code`: Original PSID variable code
- `label`: Textual description
- `category`: Thematic category
- `dtype`: Inferred data type
- `required`: 1 if essential variable, 0 otherwise
- `transform`: Potential transformation to apply

**fam_wlth_inventory.csv** - Module inventory
```csv
year,module,file_path,num_variables,num_rows
2009,FAM,sorted_data/FAM2009ER_full.csv,723,9144
1999,WLTH,sorted_data/WLTH1999_full.csv,412,7406
```

---

### 3️⃣ psid_tool.py - Canonical Grid (Variables × Years)

**Objective**: Create pivoted matrix with variables in rows and years in columns.

#### Features
- Pivot `mapping_long.csv` into wide format
- Resolve variable conflicts (WLTH preference by default)
- Add `row` column (1-based numbering)
- Optional filtering by years
- Handle duplicates and missing variables

#### Usage
```bash
python psid_tool.py \
  --mapping out/mapping_long.csv \
  --out-dir out \
  --prefer WLTH           # Prefer WLTH in case of conflict
  --years 1999,2001,2009  # Optional: filter by years
```

#### Output: canonical_grid.csv

Format:
```csv
row,concept,required,1999,2001,2003,2005,2007,2009,...,2023
1,family_id,1,ER30001,ER31001,ER32001,...,ER42001
2,person_id,1,ER30002,ER31002,ER32002,...,ER42002
3,ira_balance,0,S517,S617,S717,S817,,ER46946
```

Columns:
- `row`: Line number (canonical order)
- `concept`: Normalized concept name
- `required`: Essential variable indicator
- `{YEAR}`: Variable code for each year (empty if absent)

---

### 4️⃣ merge_grid.py - Merge Similar Rows

**Objective**: Combine multiple rows of similar variables into a single consolidated row.

#### Features
- Reads merge rules from `merge_groups.txt`
- Merges variable codes by priority (left → right)
- Keeps first non-empty value per year
- Adds `_merged` suffix to merged concepts

#### Usage
```bash
python merge_grid.py \
  --file out/canonical_grid.csv \
  --out out/canonical_grid_merged.csv \
  < merge_groups.txt

# Or via stdin
cat merge_groups.txt | python merge_grid.py \
  --file out/canonical_grid.csv \
  --out out/canonical_grid_merged.csv
```

#### merge_groups.txt Format

Each line defines a merge group:
```
ira_balance ira_any ira_num
wealth_wo_equity home_equity
vehicles vehicle
```

Rules:
- Separate concepts with spaces
- First concept becomes merged name (+ `_merged`)
- For each year, takes first non-empty value from left to right
- Original rows are removed, single merged row created

#### Example

**Before merge:**
```csv
concept,1999,2001,2003
ira_balance,S517,,S717
ira_any,,S618,
```

**Rule:** `ira_balance ira_any`

**After merge:**
```csv
concept,1999,2001,2003
ira_balance_merged,S517,S618,S717
```

#### Output
- `canonical_grid_merged.csv`
- Automatically copied to `final_grid.csv` by `run_all.sh`

---

### 5️⃣ build_final_panel.py - Optimized Parquet Panel

**Objective**: Generate final panel in partitioned Parquet format for efficient analysis.

#### Key Features
- Optimized chunk-by-chunk reading
- Automatic type downcasting (Int32, float32, category)
- ZSTD compression
- Partitioning by year (and optionally by module)
- Detailed logging with execution times
- Support for `--rebuild` to rebuild from scratch

#### Usage
```bash
python build_final_panel.py \
  --data-dir sorted_data \
  --out-dir out \
  --final-dir final_results \
  --rebuild                    # Optional: delete and rebuild
  --partition-by-module        # Optional: also partition by FAM/WLTH
```

#### Internal Process
1. **Discovery**: Scan `sorted_data/` for `*_full.csv` files
2. **Mapping**: Build mappings per (year, module)
3. **Chunk processing**:
   - For each (year, module):
     - Read only necessary columns
     - Rename according to mapping
     - Downcast types
     - Write to partitioned Parquet
4. **Manifest**: Generate output statistics

#### Memory Optimizations
- **Copy-on-write**: `pd.options.mode.copy_on_write = True`
- **Aggressive downcasting**:
  - int → Int32 (nullable)
  - float → float32
  - repetitive strings → category
- **Garbage collection**: `gc.collect()` after each chunk
- **Streaming**: Only one chunk in memory at a time

#### Output: panel.parquet/

Partitioned Parquet directory structure:
```
final_results/panel.parquet/
├── year=1999/
│   ├── module=FAM/
│   │   └── part-0.parquet
│   └── module=WLTH/
│       └── part-0.parquet
├── year=2001/
│   ├── module=FAM/
│   │   └── part-0.parquet
│   └── module=WLTH/
│       └── part-0.parquet
...
└── _common_metadata
```

Column format:
- `year` (int32): Survey year
- `module` (category): FAM or WLTH
- Canonical variables (optimized types)

#### Performance
- 10-50x faster than CSV for reading
- Compression ~70-80% compared to CSV
- Ultra-fast filtered queries via Parquet predicates

---

### 6️⃣ build_panel_parent_child.py - Parent-Child Panel

**Objective**: Build panel focused on family relationships (parent → child).

#### Features
- Extract "required" variables from `final_grid.csv`
- Identify parent-child links via `mother_id`, `father_id`
- Filter to keep only families with children
- Generate multiple complementary views
- Wide format per family (variables × years)

#### Usage
```bash
python build_panel_parent_child.py \
  --final-grid out/final_grid.csv \
  --mapping out/mapping_long.csv \
  --data-dir sorted_data \
  --out-dir final_results \
  --prefer WLTH                       # Prefer WLTH in case of conflict
  --write-family-files                # Optional: 1 CSV per family
```

#### Outputs in final_results/

**1. panel_parent_child.csv** - Individual long panel
```csv
year,family_id,person_id,mother_id,father_id,concept1,concept2,...
2009,100001,101,102,103,value1,value2,...
2009,100001,102,,,value1,value2,...
2009,100001,103,,,value1,value2,...
2011,100001,101,102,103,value1,value2,...
```

**2. parent_child_links.csv** - Parent-child relationships
```csv
year,family_id,person_id,mother_id,father_id,is_parent
2009,100001,101,102,103,False
2009,100001,102,,,True
2009,100001,103,,,True
```

**3. panel_summary.csv** - Descriptive statistics
```csv
concept,non_missing,mean,median,std
ira_balance,12450,45678.32,28000.0,51234.12
num_children,24850,2.3,2.0,1.2
```

**4. codes_resolved_audit.csv** - Correspondence log
```csv
concept,year,var_code,file_type
family_id,2009,ER42001,FAM
ira_balance,1999,S517,WLTH
```

**5. ★ panel_grid_by_family.csv** - Wide format per family
```csv
family_id,concept,1999,2001,2003,2005,2007,2009,...
100001,family_id,100001,100001,100001,100001,100001,100001,...
100001,num_children,2,2,3,3,3,2,...
100001,ira_balance,15000,18000,22000,28000,35000,42000,...
100002,family_id,100002,100002,100002,100002,100002,100002,...
100002,num_children,1,1,1,2,2,2,...
```

Structure:
- Conceptual multi-level index: (family_id, concept)
- One row per (family, concept)
- Columns = years
- Values = aggregation by family (priority to parents)

**6. family_grids/{family_id}.csv** (optional with --write-family-files)

One CSV file per family:
```
family_grids/
├── 100001.csv
├── 100002.csv
├── 100003.csv
...
```

Each file contains that family's grid only:
```csv
concept,1999,2001,2003,2005,...
family_id,100001,100001,100001,100001,...
num_children,2,2,3,3,...
ira_balance,15000,18000,22000,28000,...
```

#### Aggregation Rules

For each (family, year, concept):
1. If variable identifiable from parent → take parent value
2. Otherwise, take first non-missing value from any member
3. If no value available → `pd.NA`

---

## 📄 Configuration Files

### file_list.txt

List of SAS/TXT pairs to convert to CSV.

**Format**:
```
FAM2009ER.sas
FAM2009ER.txt
FAM2011ER.sas
FAM2011ER.txt
WLTH1999.sas
WLTH1999.txt
...
```

**Rules**:
- One line per file
- Always in pairs: `.sas` then `.txt`
- Relative or absolute names
- Paths resolved from `sorted_data/`

### merge_groups.txt

Defines groups of variables to merge.

**Format**:
```
concept1 concept2 concept3
concept4 concept5
```

**Rules**:
- One line = one merge group
- Concepts separated by spaces
- First concept = base name (+ `_merged`)
- Merge by priority left → right

**Realistic example**:
```
ira_balance ira_any ira_num ira_contrib
wealth_wo_equity home_equity other_assets
mortgage debt vehicle_loan
```

---

## 🚀 Usage

### Complete Pipeline Execution

The **recommended** method is to use the master script:

```bash
# Make script executable (once only)
chmod +x run_all.sh

# Launch complete pipeline
./run_all.sh
```

The script:
1. Checks for directory presence
2. Executes 5 steps in order
3. Displays colorized logs with timestamps
4. Stops on error
5. Shows final summary with total execution time

### Verbose / Quiet Mode

```bash
# Quiet mode (errors only)
QUIET=1 ./run_all.sh

# Very verbose mode
VERBOSE=1 ./run_all.sh

# Combination
VERBOSE=0 QUIET=1 ./run_all.sh
```

### Partial Execution

If you want to re-run only certain steps:

```bash
# Step 1 only (conversion)
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data

# Step 2 only (mapping)
python create_mapping.py --data-dir sorted_data --out-dir out

# Step 3 only (grid)
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH

# Step 4 only (merge)
python merge_grid.py --file out/canonical_grid.csv --out out/canonical_grid_merged.csv < merge_groups.txt
cp out/canonical_grid_merged.csv out/final_grid.csv

# Step 5 only (final panel)
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results --rebuild
```

### Rebuild Mode (Complete Reconstruction)

To force rebuild from scratch:

```bash
# Clean all intermediate files
rm -rf out/* final_results/*

# Re-run pipeline
./run_all.sh
```

Or target only final panel:

```bash
python build_final_panel.py \
  --data-dir sorted_data \
  --out-dir out \
  --final-dir final_results \
  --rebuild  # Force deletion and recreation of panel.parquet/
```

---

## 📤 Output Formats

### 1. Parquet Panel (recommended for analysis)

**File**: `final_results/panel.parquet/`

**Reading in Python**:

```python
import pandas as pd

# Complete read (watch memory!)
df = pd.read_parquet('final_results/panel.parquet')

# Read single year (very fast thanks to partitioning)
df_2009 = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('year', '==', 2009)]
)

# Read multiple years
df_recent = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('year', 'in', [2015, 2017, 2019, 2021, 2023])]
)

# Read WLTH module only
df_wlth = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('module', '==', 'WLTH')]
)

# Read specific columns (very efficient)
df_subset = pd.read_parquet(
    'final_results/panel.parquet',
    columns=['year', 'family_id', 'ira_balance', 'num_children']
)

# Combined filters
df_filtered = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[
        ('year', '>=', 2009),
        ('module', '==', 'WLTH')
    ],
    columns=['year', 'family_id', 'ira_balance']
)
```

**Reading in R** (with arrow):

```r
library(arrow)

# Complete read
df <- read_parquet("final_results/panel.parquet")

# Read with filters
df_2009 <- open_dataset("final_results/panel.parquet") %>%
  filter(year == 2009) %>%
  collect()

# Optimized read with dplyr
df_filtered <- open_dataset("final_results/panel.parquet") %>%
  filter(year >= 2009, module == "WLTH") %>%
  select(year, family_id, ira_balance) %>%
  collect()
```

### 2. Panel by Family (wide format)

**File**: `final_results/panel_grid_by_family.csv`

**Structure**:
- Rows: (family_id, concept)
- Columns: years
- Ideal for longitudinal analysis by family

**Reading**:

```python
import pandas as pd

# Load panel
panel = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extract specific family
family_100001 = panel[panel['family_id'] == '100001']

# Pivot for analysis
pivot = family_100001.set_index('concept').drop(columns=['family_id'])

# Access specific variable for all families
ira_evolution = panel[panel['concept'] == 'ira_balance'].set_index('family_id')
```

### 3. Individual Long Panel

**File**: `final_results/panel_parent_child.csv`

**Structure**:
- Classic long format (panel data)
- Rows: observations (person × year)
- Columns: year, family_id, person_id, mother_id, father_id, variables...

**Reading**:

```python
import pandas as pd

# Load long panel
panel_long = pd.read_csv('final_results/panel_parent_child.csv')

# Statistics by year
stats_by_year = panel_long.groupby('year').agg({
    'ira_balance': ['mean', 'median', 'std'],
    'num_children': ['mean', 'sum']
})

# Filter parents only
parents = panel_long[
    panel_long['person_id'].isin(panel_long['mother_id']) |
    panel_long['person_id'].isin(panel_long['father_id'])
]

# Panel by family
family_panel = panel_long.groupby(['family_id', 'year']).first()
```

---

## ⚡ Memory Optimizations

The pipeline implements several optimization strategies for handling large datasets:

### 1. Automatic Type Downcasting

```python
# Before optimization
df['year'] = df['year'].astype('int64')     # 8 bytes per value
df['value'] = df['value'].astype('float64') # 8 bytes per value

# After optimization
df['year'] = df['year'].astype('int32')     # 4 bytes per value (-50%)
df['value'] = df['value'].astype('float32') # 4 bytes per value (-50%)
```

**Typical gain**: 40-60% memory reduction

### 2. Categorical Types for Repetitive Variables

```python
# Before
df['module'] = df['module'].astype('string')  # ~6 bytes × nb_rows

# After
df['module'] = df['module'].astype('category')  # ~1 byte × nb_rows + dict
```

**Gain**: 80-90% for columns with few unique values

### 3. Nullable Integers (Int32 vs int32)

```python
# Use Int32 (nullable) instead of float to preserve NaN
df['count'] = df['count'].astype('Int32')
```

**Advantages**:
- Preserves missing values without float conversion
- Memory savings vs float64

### 4. Copy-on-Write

```python
# Activated globally
pd.options.mode.copy_on_write = True
```

**Advantages**:
- Avoids implicit copies
- Reduces memory peaks

### 5. Chunk Processing

```python
# Instead of loading everything into memory
for chunk in pd.read_csv('huge_file.csv', chunksize=10000):
    process(chunk)
    write_to_parquet(chunk)
    del chunk
    gc.collect()  # Explicit cleanup
```

### 6. Selective Columns (usecols)

```python
# Read only necessary columns
df = pd.read_csv('data.csv', usecols=['col1', 'col2', 'col3'])
```

**Gain**: Proportional to used columns / total columns ratio

### Summary of Gains

| Technique | Typical Memory Gain |
|-----------|---------------------|
| Downcast int64→Int32 | 50% |
| Downcast float64→float32 | 50% |
| String→Category (module, year) | 85% |
| Copy-on-write | 20-30% |
| Usecols (50% of columns) | 50% |
| **CUMULATIVE TOTAL** | **70-85%** |

**Realistic example**:
- Raw dataset: 8 GB in memory
- After optimizations: 1.2 - 2.4 GB
- Parquet panel with ZSTD compression: 300-500 MB on disk

---

## 🛠️ Troubleshooting

### Common Problems and Solutions

#### 1. **Error: "No module named 'pyarrow'"**

**Cause**: Missing Parquet dependency

**Solution**:
```bash
pip install pyarrow
# Or alternative
pip install fastparquet
```

#### 2. **MemoryError during execution**

**Cause**: Dataset too large for available RAM

**Solutions**:

A. Reduce number of years:
```bash
# Edit file_list.txt to keep only a few years
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
```

B. Increase chunksize:
```python
# In build_final_panel.py, modify:
chunksize = 5000  # Instead of 10000
```

C. Use swap/virtual memory (Linux):
```bash
# Create 8 GB swap file
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### 3. **FileNotFoundError: final_grid.csv**

**Cause**: Previous step not executed or failed

**Solution**: Re-run complete pipeline
```bash
./run_all.sh
```

Or manually run missing steps:
```bash
python psid_tool.py --mapping out/mapping_long.csv --out-dir out
python merge_grid.py --file out/canonical_grid.csv --out out/canonical_grid_merged.csv < merge_groups.txt
cp out/canonical_grid_merged.csv out/final_grid.csv
```

#### 4. **Empty columns in panel.parquet**

**Cause**: Variables not present in source files

**Diagnosis**:
```bash
# Check mapping_long.csv
cat out/mapping_long.csv | grep "variable_name"

# Check canonical_grid.csv
cat out/canonical_grid.csv | grep "variable_name"
```

**Solution**: Verify that source files actually contain these variables

#### 5. **Error "cannot concatenate object of type"**

**Cause**: Type inconsistency between chunks

**Solution**: Force dtype='string' everywhere
```python
# In script, add:
df = pd.read_csv(path, dtype='string', low_memory=False)
```

#### 6. **Very long execution time**

**Possible causes**:
- Too many files to convert
- No progress bars (tqdm)
- Slow disk

**Solutions**:

A. Install tqdm:
```bash
pip install tqdm
```

B. Manually parallelize:
```bash
# Convert FAM and WLTH in parallel in 2 terminals
# Terminal 1
python sas_to_csv.py --pattern "FAM*" --out-dir sorted_data

# Terminal 2
python sas_to_csv.py --pattern "WLTH*" --out-dir sorted_data
```

C. Use SSD if possible

#### 7. **Encoding errors during reading**

**Cause**: Special characters in SAS/TXT files

**Solution**:
```python
# Force encoding
df = pd.read_fwf(path, encoding='latin-1')  # or 'cp1252'
```

#### 8. **Permission denied when running run_all.sh**

**Cause**: File not executable

**Solution**:
```bash
chmod +x run_all.sh
./run_all.sh
```

---

## 💡 Usage Examples

### Example 1: Wealth Evolution Analysis

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load wealth data (WLTH)
df = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('module', '==', 'WLTH')],
    columns=['year', 'family_id', 'ira_balance', 'wealth_wo_equity']
)

# Calculate average wealth by year
wealth_by_year = df.groupby('year').agg({
    'ira_balance': 'mean',
    'wealth_wo_equity': 'mean'
}).reset_index()

# Visualization
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(wealth_by_year['year'], wealth_by_year['ira_balance'],
        marker='o', label='Average IRA')
ax.plot(wealth_by_year['year'], wealth_by_year['wealth_wo_equity'],
        marker='s', label='Wealth (without equity)')
ax.set_xlabel('Year')
ax.set_ylabel('Amount ($)')
ax.set_title('Average Wealth Evolution 1999-2023')
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('wealth_evolution.png', dpi=300)
```

### Example 2: Comparison Families With/Without Children

```python
import pandas as pd
import numpy as np

# Load parent-child panel
panel = pd.read_csv('final_results/panel_parent_child.csv')

# Identify families with children
families_with_kids = panel[
    panel['mother_id'].notna() | panel['father_id'].notna()
]['family_id'].unique()

# Create flag
panel['has_children'] = panel['family_id'].isin(families_with_kids)

# Comparative statistics
comparison = panel.groupby(['year', 'has_children']).agg({
    'ira_balance': ['mean', 'median'],
    'num_children': 'mean',
    'family_id': 'nunique'
}).round(2)

print(comparison)
```

### Example 3: Longitudinal Tracking of One Family

```python
import pandas as pd

# Load grid by family
panel_wide = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extract specific family
family_id = '100001'
family_data = panel_wide[panel_wide['family_id'] == family_id]

# Pivot to have concepts as index, years as columns
family_pivot = family_data.set_index('concept').drop(columns=['family_id'])

# Display evolution
print(f"Evolution of family {family_id}:")
print(family_pivot.T)  # Transpose for years as rows

# Calculate growth rates
numeric_vars = ['ira_balance', 'wealth_wo_equity']
for var in numeric_vars:
    if var in family_pivot.index:
        series = pd.to_numeric(family_pivot.loc[var], errors='coerce')
        growth = series.pct_change() * 100
        print(f"\nAnnual growth {var}:")
        print(growth.dropna().round(2))
```

### Example 4: Export for Stata/R

```python
import pandas as pd

# Load long panel
df = pd.read_csv('final_results/panel_parent_child.csv')

# Export Stata
df.to_stata('panel_for_stata.dta', write_index=False, version=118)

# Export R (RDS)
import pyreadr
pyreadr.write_rds('panel_for_r.rds', df)

# Optimized CSV export
df.to_csv('panel_optimized.csv', index=False, compression='gzip')
```

### Example 5: Complex Queries on Parquet

```python
import pandas as pd
import pyarrow.parquet as pq

# Open Parquet dataset
dataset = pq.ParquetDataset('final_results/panel.parquet')

# Complex query with multiple filters
table = dataset.read(
    columns=['year', 'family_id', 'ira_balance', 'num_children'],
    filters=[
        ('year', '>=', 2009),
        ('year', '<=', 2019),
        ('module', '==', 'FAM')
    ]
)

# Convert to pandas
df = table.to_pandas()

# Analysis
summary = df.groupby('year').agg({
    'ira_balance': ['mean', 'median', 'std', 'count'],
    'num_children': 'mean',
    'family_id': 'nunique'
})

print(summary)
```

### Example 6: Outlier Detection

```python
import pandas as pd
import numpy as np

# Load data
df = pd.read_parquet(
    'final_results/panel.parquet',
    columns=['year', 'family_id', 'ira_balance']
)

# Convert to numeric
df['ira_balance'] = pd.to_numeric(df['ira_balance'], errors='coerce')

# Calculate quartiles and IQR
Q1 = df['ira_balance'].quantile(0.25)
Q3 = df['ira_balance'].quantile(0.75)
IQR = Q3 - Q1

# Detect outliers
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

outliers = df[
    (df['ira_balance'] < lower_bound) |
    (df['ira_balance'] > upper_bound)
]

print(f"Number of outliers: {len(outliers)}")
print(f"% of outliers: {len(outliers)/len(df)*100:.2f}%")
print("\nOutlier examples:")
print(outliers.head(10))
```

---

## 📊 Complete Data Schema

### Table Relationships

```
mapping_long.csv (dictionary)
    │
    ├─→ canonical_grid.csv (pivot)
    │       │
    │       └─→ final_grid.csv (after merge)
    │               │
    │               ├─→ panel.parquet/ (optimized data)
    │               │
    │               └─→ panel_grid_by_family.csv (wide by family)
    │
    └─→ codes_resolved_audit.csv (audit)

sorted_data/*_full.csv (sources)
    │
    └─→ fam_wlth_inventory.csv (inventory)
```

### Cardinalities

- **mapping_long.csv**: ~50,000 - 200,000 rows (depends on years × variables)
- **canonical_grid.csv**: ~500 - 2,000 rows (unique concepts)
- **panel.parquet/**: 1M - 50M rows (depends on years and families)
- **panel_grid_by_family.csv**: ~10,000 - 500,000 rows (families × concepts)

---

## 🔐 Privacy Considerations

PSID data may contain sensitive information. Best practices:

1. **Never commit data** to a Git repo
   ```bash
   # Add to .gitignore
   sorted_data/
   out/
   final_results/
   *.csv
   *.parquet
   ```

2. **Encrypt data at rest** (recommended)
   ```bash
   # Example with GPG
   tar czf - final_results/ | gpg -c > final_results.tar.gz.gpg
   ```

3. **Control file access**
   ```bash
   chmod 600 final_results/*.csv
   chmod 700 final_results/panel.parquet/
   ```

---

## 📚 PSID Resources

- **Official site**: https://psidonline.isr.umich.edu/
- **Variable documentation**: https://simba.isr.umich.edu/default.aspx
- **User Guide**: https://psidonline.isr.umich.edu/Guide/default.aspx
- **FAQs**: https://psidonline.isr.umich.edu/FAQ/

---

## 🤝 Contributing

To report a bug or suggest an improvement:

1. Check that the issue doesn't already exist
2. Create an issue with:
   - Problem/improvement description
   - Reproduction steps (if bug)
   - Error logs
   - Python version and dependencies

---

## 📝 License

This project is an academic research tool. PSID data is subject to its own usage license.

---

## ✨ Changelog

### Current Version (2024)

**New Features:**
- Complete automated pipeline via `run_all.sh`
- Parquet support with ZSTD compression
- Aggressive memory optimizations (Int32, float32, category)
- Colorized logging with timestamps
- Progress bar support (tqdm)
- Rebuild mode for panel.parquet

**Main Scripts:**
- `sas_to_csv.py`: Optimized SAS/TXT conversion
- `create_mapping.py`: Mapping with advanced normalization
- `psid_tool.py`: Canonical grid with conflict resolution
- `merge_grid.py`: Configurable row merging
- `build_final_panel.py`: Memory-efficient Parquet panel
- `build_panel_parent_child.py`: Family-child panel with relationships

**Improvements:**
- Robust error handling
- Comprehensive documentation
- Copy-on-write for memory reduction
- Intelligent partitioning by year/module

---

## 📞 Support

For technical questions:
1. Consult the [Troubleshooting](#troubleshooting) section
2. Check [Usage Examples](#usage-examples)
3. Read script docstrings (header of each .py file)

---

**Last updated**: 2024-11-01
**Version**: 3.0
**Author**: PSID Pipeline RA Team
