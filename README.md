# Pipeline PSID - Traitement et Construction de Panels de Données

## 📋 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture du projet](#architecture-du-projet)
3. [Installation et prérequis](#installation-et-prérequis)
4. [Structure des données](#structure-des-données)
5. [Pipeline complet](#pipeline-complet)
6. [Scripts détaillés](#scripts-détaillés)
7. [Fichiers de configuration](#fichiers-de-configuration)
8. [Utilisation](#utilisation)
9. [Formats de sortie](#formats-de-sortie)
10. [Optimisations mémoire](#optimisations-mémoire)
11. [Dépannage](#dépannage)
12. [Exemples d'utilisation](#exemples-dutilisation)

---

## 🎯 Vue d'ensemble

Ce projet constitue un **pipeline complet et optimisé** pour le traitement des données du **Panel Study of Income Dynamics (PSID)**. Il transforme les fichiers bruts SAS/TXT en panels structurés et exploitables, avec un accent particulier sur:

- **L'efficacité mémoire** : gestion de datasets de plusieurs dizaines de millions de lignes
- **La traçabilité** : logging détaillé de chaque étape
- **La flexibilité** : configuration via fichiers externes
- **La performance** : sortie en format Parquet partitionné avec compression ZSTD

### Objectifs principaux

1. **Conversion** : Transformer les fichiers SAS/TXT en CSV exploitables
2. **Mapping** : Créer une correspondance canonique entre variables et années
3. **Grille canonique** : Construire une matrice variables × années
4. **Fusion** : Combiner des variables similaires selon des règles définies
5. **Panel final** : Générer des panels longs optimisés par famille et par année

---

## 🏗️ Architecture du projet

```
algo3/
│
├── 📂 sorted_data/              # Données sources PSID
│   ├── FAM2009ER.sas           # Scripts SAS de définition
│   ├── FAM2009ER.txt           # Données brutes (format fixe)
│   ├── FAM2009ER_full.csv      # CSV converti (généré)
│   ├── WLTH1999.sas
│   ├── WLTH1999.txt
│   └── ...                      # Autres années (1999-2023)
│
├── 📂 out/                      # Résultats intermédiaires
│   ├── mapping_long.csv         # Dictionnaire complet des variables
│   ├── fam_wlth_inventory.csv  # Inventaire des modules FAM/WLTH
│   ├── canonical_grid.csv       # Grille canonique initiale
│   ├── canonical_grid_merged.csv # Grille après fusion de lignes
│   └── final_grid.csv           # Grille finale utilisée pour l'extraction
│
├── 📂 final_results/            # Résultats finaux
│   ├── panel_parent_child.csv   # Panel long individuel
│   ├── parent_child_links.csv   # Relations parent-enfant
│   ├── panel_summary.csv        # Statistiques descriptives
│   ├── codes_resolved_audit.csv # Journal de correspondances
│   ├── panel_grid_by_family.csv # ★ Panel par famille (format wide)
│   ├── panel.parquet/           # ★ Panel optimisé (format partitionné)
│   └── family_grids/            # (Optionnel) Un CSV par famille
│
├── 📜 Scripts Python principaux
│   ├── sas_to_csv.py            # [1] Conversion SAS/TXT → CSV
│   ├── create_mapping.py        # [2] Construction du mapping
│   ├── psid_tool.py             # [3] Grille canonique
│   ├── merge_grid.py            # [4] Fusion de lignes
│   ├── build_final_panel.py     # [5] Panel optimisé Parquet
│   └── build_panel_parent_child.py # Panel famille-enfant
│
├── 📜 Scripts utilitaires
│   ├── filter_grid_rows.py      # Filtrage de la grille
│   ├── make_canonical_grid.py   # Alternative pour grille canonique
│   ├── no_children_from_gid.py  # Analyse GID sans enfants
│   ├── sas_to_csv_gid.py        # Conversion GID spécifique
│   └── build_parent_child_presence_matrix.py # Matrice de présence
│
├── 📜 Fichiers de configuration
│   ├── file_list.txt            # Liste des fichiers à convertir
│   └── merge_groups.txt         # Règles de fusion de variables
│
├── 📜 Orchestration
│   ├── run_all.sh               # ★ Script maître exécutant tout le pipeline
│   └── README.md                # Ce fichier
│
└── 📂 .vscode/                  # Configuration VSCode
    └── extensions.json
```

---

## 💻 Installation et prérequis

### Prérequis système

- **Python 3.8+** (testé avec 3.9 et 3.10)
- **8 GB RAM minimum** (16 GB recommandé pour les gros datasets)
- **10 GB d'espace disque** pour les fichiers intermédiaires et finaux

### Dépendances Python

```bash
# Dépendances principales
pip install pandas>=1.5.0
pip install numpy>=1.23.0
pip install pyarrow>=10.0.0  # Pour le format Parquet

# Optionnel mais recommandé
pip install tqdm              # Barres de progression
pip install fastparquet       # Alternative à pyarrow
```

### Installation complète avec environnement virtuel

```bash
# Créer un environnement virtuel
python3 -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# Installer les dépendances
pip install --upgrade pip
pip install pandas numpy pyarrow tqdm

# Vérifier l'installation
python -c "import pandas; import pyarrow; print('OK')"
```

---

## 📊 Structure des données

### Sources PSID

Le projet traite deux types principaux de modules PSID:

#### 1. **Module FAM (Family)** - Données démographiques et familiales
- Variables familiales (taille du ménage, composition)
- Caractéristiques du chef de famille
- Informations sur les enfants
- Format: `FAM{YEAR}ER.txt` et `FAM{YEAR}ER.sas`
- Années disponibles: 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023

#### 2. **Module WLTH (Wealth)** - Données patrimoniales
- Actifs (immobilier, actions, épargne)
- Dettes (hypothèques, prêts)
- IRA et comptes de retraite
- Format: `WLTH{YEAR}.txt` et `WLTH{YEAR}.sas`
- Années disponibles: 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, ...

### Format des fichiers sources

**Fichiers SAS (.sas)**
- Scripts de lecture définissant les positions et largeurs des colonnes
- Contiennent les métadonnées (noms de variables, types, labels)

**Fichiers TXT (.txt)**
- Données brutes en format largeur fixe (Fixed-Width Format)
- Pas de séparateurs, positions définies par le fichier .sas
- Exemple: une ligne = un enregistrement, chaque variable à position fixe

---

## 🔄 Pipeline complet

Le pipeline s'exécute en **5 étapes séquentielles** via le script `run_all.sh`:

```
[1] SAS/TXT → CSV  →  [2] Mapping  →  [3] Grille canonique  →  [4] Fusion  →  [5] Panel final
```

### Diagramme de flux

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

## 📝 Scripts détaillés

### 1️⃣ sas_to_csv.py - Conversion SAS/TXT vers CSV

**Objectif**: Convertir les fichiers bruts PSID (TXT à largeur fixe) en CSV exploitables.

#### Fonctionnalités
- Parse les scripts SAS (.sas) pour extraire les métadonnées de colonnes
- Lit les fichiers TXT avec `pd.read_fwf()` (fixed-width format)
- Ajoute une ligne de labels (2e ligne du CSV)
- Gestion des erreurs de parsing avec fallback
- Barres de progression (si tqdm installé)

#### Utilisation
```bash
# Mode manuel
python sas_to_csv.py \
  --file-list file_list.txt \
  --out-dir sorted_data

# Via run_all.sh
./run_all.sh  # Étape 1 automatique
```

#### Entrées
- `file_list.txt`: Liste des fichiers à convertir
  ```
  FAM2009ER.sas
  FAM2009ER.txt
  WLTH1999.sas
  WLTH1999.txt
  ```

#### Sorties
- `sorted_data/FAM{YEAR}ER_full.csv`
- `sorted_data/WLTH{YEAR}_full.csv`
- Structure:
  - Ligne 1: Noms de variables (codes)
  - Ligne 2: Labels descriptifs
  - Lignes suivantes: Données

#### Performance
- ~1-2 minutes par fichier (variable selon taille)
- Logging colorisé avec temps d'exécution

---

### 2️⃣ create_mapping.py - Construction du dictionnaire de variables

**Objectif**: Créer une table de correspondance exhaustive entre variables PSID et leur définition.

#### Fonctionnalités
- Analyse les headers de tous les CSV générés
- Extrait les labels de la 2e ligne
- Normalise les noms de variables en concepts canoniques
- Détecte automatiquement le type (FAM/WLTH)
- Applique des règles de synonymes et normalisation
- Devine les types de données (numeric/string)

#### Utilisation
```bash
python create_mapping.py \
  --data-dir sorted_data \
  --out-dir out
```

#### Sorties principales

**mapping_long.csv** - Format long exhaustif
```csv
canonical,year,file_type,var_code,label,category,dtype,required,transform
family_id,2009,FAM,ER42001,"Family ID",Demographics,string,1,
num_children,2009,FAM,ER42003,"Number of children",Demographics,int,1,
ira_balance,1999,WLTH,S517,"IRA Balance",Retirement/IRA,float,0,
```

Colonnes:
- `canonical`: Nom de concept normalisé
- `year`: Année de l'enquête
- `file_type`: FAM ou WLTH
- `var_code`: Code variable PSID original
- `label`: Description textuelle
- `category`: Catégorie thématique
- `dtype`: Type de données inféré
- `required`: 1 si variable indispensable, 0 sinon
- `transform`: Transformation éventuelle à appliquer

**fam_wlth_inventory.csv** - Inventaire des modules
```csv
year,module,file_path,num_variables,num_rows
2009,FAM,sorted_data/FAM2009ER_full.csv,723,9144
1999,WLTH,sorted_data/WLTH1999_full.csv,412,7406
```

---

### 3️⃣ psid_tool.py - Grille canonique (Variables × Années)

**Objectif**: Créer une matrice pivotée avec les variables en lignes et les années en colonnes.

#### Fonctionnalités
- Pivote `mapping_long.csv` en format wide
- Résout les conflits de variables (préférence WLTH par défaut)
- Ajoute une colonne `row` (numérotation 1-based)
- Filtre optionnel par années
- Gère les doublons et variables manquantes

#### Utilisation
```bash
python psid_tool.py \
  --mapping out/mapping_long.csv \
  --out-dir out \
  --prefer WLTH           # Préférer WLTH en cas de conflit
  --years 1999,2001,2009  # Optionnel: filtrer par années
```

#### Sortie: canonical_grid.csv

Format:
```csv
row,concept,required,1999,2001,2003,2005,2007,2009,...,2023
1,family_id,1,ER30001,ER31001,ER32001,...,ER42001
2,person_id,1,ER30002,ER31002,ER32002,...,ER42002
3,ira_balance,0,S517,S617,S717,S817,,ER46946
```

Colonnes:
- `row`: Numéro de ligne (ordre canonique)
- `concept`: Nom du concept normalisé
- `required`: Indicateur de variable indispensable
- `{YEAR}`: Code variable pour chaque année (vide si absent)

---

### 4️⃣ merge_grid.py - Fusion de lignes similaires

**Objectif**: Combiner plusieurs lignes de variables similaires en une seule ligne consolidée.

#### Fonctionnalités
- Lit les règles de fusion depuis `merge_groups.txt`
- Fusionne les codes de variables par priorité (gauche → droite)
- Conserve la première valeur non vide par année
- Ajoute un suffixe `_merged` aux concepts fusionnés

#### Utilisation
```bash
python merge_grid.py \
  --file out/canonical_grid.csv \
  --out out/canonical_grid_merged.csv \
  < merge_groups.txt

# Ou via stdin
cat merge_groups.txt | python merge_grid.py \
  --file out/canonical_grid.csv \
  --out out/canonical_grid_merged.csv
```

#### Format merge_groups.txt

Chaque ligne définit un groupe de fusion:
```
ira_balance ira_any ira_num
wealth_wo_equity home_equity
vehicles vehicle
```

Règles:
- Séparer les concepts par espaces
- Le premier concept devient le nom fusionné (+ `_merged`)
- Pour chaque année, prend la première valeur non vide de gauche à droite
- Les lignes originales sont supprimées, une seule ligne fusionnée créée

#### Exemple

**Avant fusion:**
```csv
concept,1999,2001,2003
ira_balance,S517,,S717
ira_any,,S618,
```

**Règle:** `ira_balance ira_any`

**Après fusion:**
```csv
concept,1999,2001,2003
ira_balance_merged,S517,S618,S717
```

#### Sortie
- `canonical_grid_merged.csv`
- Copié automatiquement vers `final_grid.csv` par `run_all.sh`

---

### 5️⃣ build_final_panel.py - Panel optimisé en Parquet

**Objectif**: Générer le panel final en format Parquet partitionné pour une analyse efficace.

#### Fonctionnalités clés
- Lecture optimisée chunk par chunk
- Downcast automatique des types (Int32, float32, category)
- Compression ZSTD
- Partitionnement par année (et optionnellement par module)
- Logging détaillé avec temps d'exécution
- Support de `--rebuild` pour reconstruire depuis zéro

#### Utilisation
```bash
python build_final_panel.py \
  --data-dir sorted_data \
  --out-dir out \
  --final-dir final_results \
  --rebuild                    # Optionnel: effacer et reconstruire
  --partition-by-module        # Optionnel: partitionner aussi par FAM/WLTH
```

#### Processus interne
1. **Découverte** : Scan `sorted_data/` pour fichiers `*_full.csv`
2. **Mapping** : Construit des mappings par (année, module)
3. **Traitement chunk** :
   - Pour chaque (année, module):
     - Lit seulement les colonnes nécessaires
     - Renomme selon mapping
     - Downcast des types
     - Écrit en Parquet partitionné
4. **Manifest** : Génère des statistiques de sortie

#### Optimisations mémoire
- **Copy-on-write** : `pd.options.mode.copy_on_write = True`
- **Downcasting agressif** :
  - int → Int32 (nullable)
  - float → float32
  - string répétitifs → category
- **Garbage collection** : `gc.collect()` après chaque chunk
- **Streaming** : Un seul chunk en mémoire à la fois

#### Sortie: panel.parquet/

Structure du répertoire Parquet partitionné:
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

Format des colonnes:
- `year` (int32): Année d'enquête
- `module` (category): FAM ou WLTH
- Variables canoniques (types optimisés)

#### Performance
- 10-50x plus rapide que CSV à la lecture
- Compression ~70-80% par rapport à CSV
- Requêtes filtrées ultra-rapides via prédicats Parquet

---

### 6️⃣ build_panel_parent_child.py - Panel parent-enfant

**Objectif**: Construire un panel centré sur les relations familiales (parent → enfant).

#### Fonctionnalités
- Extrait les variables "required" de `final_grid.csv`
- Identifie les liens parent-enfant via `mother_id`, `father_id`
- Filtre pour ne garder que les familles avec enfants
- Génère plusieurs vues complémentaires
- Format wide par famille (variables × années)

#### Utilisation
```bash
python build_panel_parent_child.py \
  --final-grid out/final_grid.csv \
  --mapping out/mapping_long.csv \
  --data-dir sorted_data \
  --out-dir final_results \
  --prefer WLTH                       # Préférer WLTH en cas de conflit
  --write-family-files                # Optionnel: 1 CSV par famille
```

#### Sorties dans final_results/

**1. panel_parent_child.csv** - Panel long individuel
```csv
year,family_id,person_id,mother_id,father_id,concept1,concept2,...
2009,100001,101,102,103,value1,value2,...
2009,100001,102,,,value1,value2,...
2009,100001,103,,,value1,value2,...
2011,100001,101,102,103,value1,value2,...
```

**2. parent_child_links.csv** - Relations parent-enfant
```csv
year,family_id,person_id,mother_id,father_id,is_parent
2009,100001,101,102,103,False
2009,100001,102,,,True
2009,100001,103,,,True
```

**3. panel_summary.csv** - Statistiques descriptives
```csv
concept,non_missing,mean,median,std
ira_balance,12450,45678.32,28000.0,51234.12
num_children,24850,2.3,2.0,1.2
```

**4. codes_resolved_audit.csv** - Journal de correspondances
```csv
concept,year,var_code,file_type
family_id,2009,ER42001,FAM
ira_balance,1999,S517,WLTH
```

**5. ★ panel_grid_by_family.csv** - Format wide par famille
```csv
family_id,concept,1999,2001,2003,2005,2007,2009,...
100001,family_id,100001,100001,100001,100001,100001,100001,...
100001,num_children,2,2,3,3,3,2,...
100001,ira_balance,15000,18000,22000,28000,35000,42000,...
100002,family_id,100002,100002,100002,100002,100002,100002,...
100002,num_children,1,1,1,2,2,2,...
```

Structure:
- Index multi-niveau conceptuel: (family_id, concept)
- Une ligne par (famille, concept)
- Colonnes = années
- Valeurs = agrégation par famille (priorité aux parents)

**6. family_grids/{family_id}.csv** (optionnel avec --write-family-files)

Un fichier CSV par famille:
```
family_grids/
├── 100001.csv
├── 100002.csv
├── 100003.csv
...
```

Chaque fichier contient la grille de cette famille uniquement:
```csv
concept,1999,2001,2003,2005,...
family_id,100001,100001,100001,100001,...
num_children,2,2,3,3,...
ira_balance,15000,18000,22000,28000,...
```

#### Règles d'agrégation

Pour chaque (famille, année, concept):
1. Si variable identifiable chez un parent → prendre valeur parent
2. Sinon, prendre la première valeur non manquante de n'importe quel membre
3. Si aucune valeur disponible → `pd.NA`

---

## 📄 Fichiers de configuration

### file_list.txt

Liste des paires SAS/TXT à convertir en CSV.

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

**Règles**:
- Une ligne par fichier
- Toujours par paire: `.sas` puis `.txt`
- Noms relatifs ou absolus
- Chemins résolus depuis `sorted_data/`

### merge_groups.txt

Définit les groupes de variables à fusionner.

**Format**:
```
concept1 concept2 concept3
concept4 concept5
```

**Règles**:
- Une ligne = un groupe de fusion
- Concepts séparés par espaces
- Premier concept = nom de base (+ `_merged`)
- Fusion par priorité gauche → droite

**Exemple réaliste**:
```
ira_balance ira_any ira_num ira_contrib
wealth_wo_equity home_equity other_assets
mortgage debt vehicle_loan
```

---

## 🚀 Utilisation

### Exécution complète du pipeline

La méthode **recommandée** est d'utiliser le script maître:

```bash
# Rendre le script exécutable (une seule fois)
chmod +x run_all.sh

# Lancer le pipeline complet
./run_all.sh
```

Le script:
1. Vérifie la présence des répertoires
2. Exécute les 5 étapes dans l'ordre
3. Affiche des logs colorisés avec timestamps
4. S'arrête en cas d'erreur
5. Affiche un résumé final avec temps d'exécution total

### Mode verbeux / silencieux

```bash
# Mode silencieux (erreurs uniquement)
QUIET=1 ./run_all.sh

# Mode très verbeux
VERBOSE=1 ./run_all.sh

# Combinaison
VERBOSE=0 QUIET=1 ./run_all.sh
```

### Exécution partielle

Si vous voulez relancer seulement certaines étapes:

```bash
# Étape 1 uniquement (conversion)
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data

# Étape 2 uniquement (mapping)
python create_mapping.py --data-dir sorted_data --out-dir out

# Étape 3 uniquement (grille)
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH

# Étape 4 uniquement (fusion)
python merge_grid.py --file out/canonical_grid.csv --out out/canonical_grid_merged.csv < merge_groups.txt
cp out/canonical_grid_merged.csv out/final_grid.csv

# Étape 5 uniquement (panel final)
python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results --rebuild
```

### Mode rebuild (reconstruction complète)

Pour forcer une reconstruction depuis zéro:

```bash
# Nettoyer tous les fichiers intermédiaires
rm -rf out/* final_results/*

# Relancer le pipeline
./run_all.sh
```

Ou cibler seulement le panel final:

```bash
python build_final_panel.py \
  --data-dir sorted_data \
  --out-dir out \
  --final-dir final_results \
  --rebuild  # Force la suppression et recréation de panel.parquet/
```

---

## 📤 Formats de sortie

### 1. Panel Parquet (recommandé pour analyse)

**Fichier**: `final_results/panel.parquet/`

**Lecture en Python**:

```python
import pandas as pd

# Lecture complète (attention à la mémoire!)
df = pd.read_parquet('final_results/panel.parquet')

# Lecture d'une seule année (très rapide grâce au partitionnement)
df_2009 = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('year', '==', 2009)]
)

# Lecture de plusieurs années
df_recent = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('year', 'in', [2015, 2017, 2019, 2021, 2023])]
)

# Lecture du module WLTH uniquement
df_wlth = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('module', '==', 'WLTH')]
)

# Lecture de colonnes spécifiques (très efficace)
df_subset = pd.read_parquet(
    'final_results/panel.parquet',
    columns=['year', 'family_id', 'ira_balance', 'num_children']
)

# Combinaison de filtres
df_filtered = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[
        ('year', '>=', 2009),
        ('module', '==', 'WLTH')
    ],
    columns=['year', 'family_id', 'ira_balance']
)
```

**Lecture en R** (avec arrow):

```r
library(arrow)

# Lecture complète
df <- read_parquet("final_results/panel.parquet")

# Lecture avec filtres
df_2009 <- open_dataset("final_results/panel.parquet") %>%
  filter(year == 2009) %>%
  collect()

# Lecture optimisée avec dplyr
df_filtered <- open_dataset("final_results/panel.parquet") %>%
  filter(year >= 2009, module == "WLTH") %>%
  select(year, family_id, ira_balance) %>%
  collect()
```

### 2. Panel par famille (format wide)

**Fichier**: `final_results/panel_grid_by_family.csv`

**Structure**:
- Lignes: (family_id, concept)
- Colonnes: années
- Idéal pour analyses longitudinales par famille

**Lecture**:

```python
import pandas as pd

# Charger le panel
panel = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extraire une famille spécifique
family_100001 = panel[panel['family_id'] == '100001']

# Pivoter pour analyse
pivot = family_100001.set_index('concept').drop(columns=['family_id'])

# Accéder à une variable spécifique pour toutes les familles
ira_evolution = panel[panel['concept'] == 'ira_balance'].set_index('family_id')
```

### 3. Panel long individuel

**Fichier**: `final_results/panel_parent_child.csv`

**Structure**:
- Format long classique (panel data)
- Lignes: observations (personne × année)
- Colonnes: year, family_id, person_id, mother_id, father_id, variables...

**Lecture**:

```python
import pandas as pd

# Charger le panel long
panel_long = pd.read_csv('final_results/panel_parent_child.csv')

# Statistiques par année
stats_by_year = panel_long.groupby('year').agg({
    'ira_balance': ['mean', 'median', 'std'],
    'num_children': ['mean', 'sum']
})

# Filtrer les parents uniquement
parents = panel_long[
    panel_long['person_id'].isin(panel_long['mother_id']) |
    panel_long['person_id'].isin(panel_long['father_id'])
]

# Panel par famille
family_panel = panel_long.groupby(['family_id', 'year']).first()
```

---

## ⚡ Optimisations mémoire

Le pipeline implémente plusieurs stratégies d'optimisation pour gérer des datasets volumineux:

### 1. Downcast automatique des types

```python
# Avant optimisation
df['year'] = df['year'].astype('int64')     # 8 bytes par valeur
df['value'] = df['value'].astype('float64') # 8 bytes par valeur

# Après optimisation
df['year'] = df['year'].astype('int32')     # 4 bytes par valeur (-50%)
df['value'] = df['value'].astype('float32') # 4 bytes par valeur (-50%)
```

**Gain typique**: 40-60% de réduction de mémoire

### 2. Types catégoriels pour variables répétitives

```python
# Avant
df['module'] = df['module'].astype('string')  # ~6 bytes × nb_rows

# Après
df['module'] = df['module'].astype('category')  # ~1 byte × nb_rows + dict
```

**Gain**: 80-90% pour colonnes avec peu de valeurs uniques

### 3. Nullable integers (Int32 vs int32)

```python
# Utilisation de Int32 (nullable) au lieu de float pour préserver les NaN
df['count'] = df['count'].astype('Int32')
```

**Avantages**:
- Conserve les valeurs manquantes sans conversion en float
- Économie de mémoire vs float64

### 4. Copy-on-write

```python
# Activé globalement
pd.options.mode.copy_on_write = True
```

**Avantages**:
- Évite les copies implicites
- Réduit les pics de mémoire

### 5. Traitement par chunks

```python
# Au lieu de tout charger en mémoire
for chunk in pd.read_csv('huge_file.csv', chunksize=10000):
    process(chunk)
    write_to_parquet(chunk)
    del chunk
    gc.collect()  # Libération explicite
```

### 6. Colonnes sélectives (usecols)

```python
# Ne lire que les colonnes nécessaires
df = pd.read_csv('data.csv', usecols=['col1', 'col2', 'col3'])
```

**Gain**: Proportionnel au ratio colonnes utilisées / colonnes totales

### Résumé des gains

| Technique | Gain mémoire typique |
|-----------|---------------------|
| Downcast int64→Int32 | 50% |
| Downcast float64→float32 | 50% |
| String→Category (module, year) | 85% |
| Copy-on-write | 20-30% |
| Usecols (50% des colonnes) | 50% |
| **TOTAL CUMULÉ** | **70-85%** |

**Exemple réaliste**:
- Dataset brut: 8 GB en mémoire
- Après optimisations: 1.2 - 2.4 GB
- Panel Parquet avec compression ZSTD: 300-500 MB sur disque

---

## 🛠️ Dépannage

### Problèmes courants et solutions

#### 1. **Erreur: "No module named 'pyarrow'"**

**Cause**: Dépendance Parquet manquante

**Solution**:
```bash
pip install pyarrow
# Ou alternative
pip install fastparquet
```

#### 2. **MemoryError lors de l'exécution**

**Cause**: Dataset trop volumineux pour la RAM disponible

**Solutions**:

A. Réduire le nombre d'années:
```bash
# Éditer file_list.txt pour ne garder que quelques années
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
```

B. Augmenter le chunksize:
```python
# Dans build_final_panel.py, modifier:
chunksize = 5000  # Au lieu de 10000
```

C. Utiliser swap/virtual memory (Linux):
```bash
# Créer un fichier de swap de 8 GB
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### 3. **FileNotFoundError: final_grid.csv**

**Cause**: Étape précédente non exécutée ou échouée

**Solution**: Relancer le pipeline complet
```bash
./run_all.sh
```

Ou manuellement les étapes manquantes:
```bash
python psid_tool.py --mapping out/mapping_long.csv --out-dir out
python merge_grid.py --file out/canonical_grid.csv --out out/canonical_grid_merged.csv < merge_groups.txt
cp out/canonical_grid_merged.csv out/final_grid.csv
```

#### 4. **Colonnes vides dans panel.parquet**

**Cause**: Variables pas présentes dans les fichiers sources

**Diagnostic**:
```bash
# Vérifier mapping_long.csv
cat out/mapping_long.csv | grep "variable_name"

# Vérifier canonical_grid.csv
cat out/canonical_grid.csv | grep "variable_name"
```

**Solution**: Vérifier que les fichiers sources contiennent bien ces variables

#### 5. **Erreur "cannot concatenate object of type"**

**Cause**: Incohérence de types entre chunks

**Solution**: Forcer dtype='string' partout
```python
# Dans le script, ajouter:
df = pd.read_csv(path, dtype='string', low_memory=False)
```

#### 6. **Temps d'exécution très long**

**Causes possibles**:
- Trop de fichiers à convertir
- Pas de barres de progression (tqdm)
- Disque lent

**Solutions**:

A. Installer tqdm:
```bash
pip install tqdm
```

B. Paralléliser manuellement:
```bash
# Convertir FAM et WLTH en parallèle dans 2 terminaux
# Terminal 1
python sas_to_csv.py --pattern "FAM*" --out-dir sorted_data

# Terminal 2
python sas_to_csv.py --pattern "WLTH*" --out-dir sorted_data
```

C. Utiliser un SSD si possible

#### 7. **Encoding errors lors de la lecture**

**Cause**: Caractères spéciaux dans fichiers SAS/TXT

**Solution**:
```python
# Forcer l'encoding
df = pd.read_fwf(path, encoding='latin-1')  # ou 'cp1252'
```

#### 8. **Permission denied lors de l'exécution de run_all.sh**

**Cause**: Fichier pas exécutable

**Solution**:
```bash
chmod +x run_all.sh
./run_all.sh
```

---

## 💡 Exemples d'utilisation

### Exemple 1: Analyse de l'évolution du patrimoine

```python
import pandas as pd
import matplotlib.pyplot as plt

# Charger les données de patrimoine (WLTH)
df = pd.read_parquet(
    'final_results/panel.parquet',
    filters=[('module', '==', 'WLTH')],
    columns=['year', 'family_id', 'ira_balance', 'wealth_wo_equity']
)

# Calculer le patrimoine moyen par année
wealth_by_year = df.groupby('year').agg({
    'ira_balance': 'mean',
    'wealth_wo_equity': 'mean'
}).reset_index()

# Visualisation
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(wealth_by_year['year'], wealth_by_year['ira_balance'],
        marker='o', label='IRA moyen')
ax.plot(wealth_by_year['year'], wealth_by_year['wealth_wo_equity'],
        marker='s', label='Patrimoine (sans equity)')
ax.set_xlabel('Année')
ax.set_ylabel('Montant ($)')
ax.set_title('Évolution du patrimoine moyen 1999-2023')
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('wealth_evolution.png', dpi=300)
```

### Exemple 2: Comparaison familles avec/sans enfants

```python
import pandas as pd
import numpy as np

# Charger le panel parent-enfant
panel = pd.read_csv('final_results/panel_parent_child.csv')

# Identifier familles avec enfants
families_with_kids = panel[
    panel['mother_id'].notna() | panel['father_id'].notna()
]['family_id'].unique()

# Créer le flag
panel['has_children'] = panel['family_id'].isin(families_with_kids)

# Statistiques comparatives
comparison = panel.groupby(['year', 'has_children']).agg({
    'ira_balance': ['mean', 'median'],
    'num_children': 'mean',
    'family_id': 'nunique'
}).round(2)

print(comparison)
```

### Exemple 3: Tracking longitudinal d'une famille

```python
import pandas as pd

# Charger la grille par famille
panel_wide = pd.read_csv('final_results/panel_grid_by_family.csv')

# Extraire une famille spécifique
family_id = '100001'
family_data = panel_wide[panel_wide['family_id'] == family_id]

# Pivoter pour avoir concepts en index, années en colonnes
family_pivot = family_data.set_index('concept').drop(columns=['family_id'])

# Afficher l'évolution
print(f"Évolution de la famille {family_id}:")
print(family_pivot.T)  # Transposer pour années en lignes

# Calculer des taux de croissance
numeric_vars = ['ira_balance', 'wealth_wo_equity']
for var in numeric_vars:
    if var in family_pivot.index:
        series = pd.to_numeric(family_pivot.loc[var], errors='coerce')
        growth = series.pct_change() * 100
        print(f"\nCroissance annuelle {var}:")
        print(growth.dropna().round(2))
```

### Exemple 4: Export pour Stata/R

```python
import pandas as pd

# Charger panel long
df = pd.read_csv('final_results/panel_parent_child.csv')

# Export Stata
df.to_stata('panel_for_stata.dta', write_index=False, version=118)

# Export R (RDS)
import pyreadr
pyreadr.write_rds('panel_for_r.rds', df)

# Export CSV optimisé
df.to_csv('panel_optimized.csv', index=False, compression='gzip')
```

### Exemple 5: Requêtes complexes sur Parquet

```python
import pandas as pd
import pyarrow.parquet as pq

# Ouvrir le dataset Parquet
dataset = pq.ParquetDataset('final_results/panel.parquet')

# Requête complexe avec filtres multiples
table = dataset.read(
    columns=['year', 'family_id', 'ira_balance', 'num_children'],
    filters=[
        ('year', '>=', 2009),
        ('year', '<=', 2019),
        ('module', '==', 'FAM')
    ]
)

# Convertir en pandas
df = table.to_pandas()

# Analyse
summary = df.groupby('year').agg({
    'ira_balance': ['mean', 'median', 'std', 'count'],
    'num_children': 'mean',
    'family_id': 'nunique'
})

print(summary)
```

### Exemple 6: Détection de valeurs aberrantes

```python
import pandas as pd
import numpy as np

# Charger données
df = pd.read_parquet(
    'final_results/panel.parquet',
    columns=['year', 'family_id', 'ira_balance']
)

# Convertir en numérique
df['ira_balance'] = pd.to_numeric(df['ira_balance'], errors='coerce')

# Calculer quartiles et IQR
Q1 = df['ira_balance'].quantile(0.25)
Q3 = df['ira_balance'].quantile(0.75)
IQR = Q3 - Q1

# Détecter outliers
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

outliers = df[
    (df['ira_balance'] < lower_bound) |
    (df['ira_balance'] > upper_bound)
]

print(f"Nombre d'outliers: {len(outliers)}")
print(f"% d'outliers: {len(outliers)/len(df)*100:.2f}%")
print("\nExemples d'outliers:")
print(outliers.head(10))
```

---

## 📊 Schéma de données complet

### Relations entre tables

```
mapping_long.csv (dictionnaire)
    │
    ├─→ canonical_grid.csv (pivot)
    │       │
    │       └─→ final_grid.csv (après fusion)
    │               │
    │               ├─→ panel.parquet/ (données optimisées)
    │               │
    │               └─→ panel_grid_by_family.csv (wide par famille)
    │
    └─→ codes_resolved_audit.csv (audit)

sorted_data/*_full.csv (sources)
    │
    └─→ fam_wlth_inventory.csv (inventaire)
```

### Cardinalités

- **mapping_long.csv**: ~50,000 - 200,000 lignes (dépend du nb d'années × variables)
- **canonical_grid.csv**: ~500 - 2,000 lignes (concepts uniques)
- **panel.parquet/**: 1M - 50M lignes (dépend des années et familles)
- **panel_grid_by_family.csv**: ~10,000 - 500,000 lignes (familles × concepts)

---

## 🔐 Considérations de confidentialité

Les données PSID peuvent contenir des informations sensibles. Bonnes pratiques:

1. **Ne jamais commiter les données** dans un repo Git
   ```bash
   # Ajouter à .gitignore
   sorted_data/
   out/
   final_results/
   *.csv
   *.parquet
   ```

2. **Chiffrer les données au repos** (recommandé)
   ```bash
   # Exemple avec GPG
   tar czf - final_results/ | gpg -c > final_results.tar.gz.gpg
   ```

3. **Contrôler l'accès** aux fichiers
   ```bash
   chmod 600 final_results/*.csv
   chmod 700 final_results/panel.parquet/
   ```

---

## 📚 Ressources PSID

- **Site officiel**: https://psidonline.isr.umich.edu/
- **Documentation des variables**: https://simba.isr.umich.edu/default.aspx
- **User Guide**: https://psidonline.isr.umich.edu/Guide/default.aspx
- **FAQs**: https://psidonline.isr.umich.edu/FAQ/

---

## 🤝 Contribution

Pour signaler un bug ou proposer une amélioration:

1. Vérifier que le problème n'existe pas déjà
2. Créer une issue avec:
   - Description du problème/amélioration
   - Étapes de reproduction (si bug)
   - Logs d'erreur
   - Version Python et dépendances

---

## 📝 Licence

Ce projet est un outil de recherche académique. Les données PSID sont soumises à leur propre licence d'utilisation.

---

## ✨ Changelog

### Version actuelle (2024)

**Nouvelles fonctionnalités:**
- Pipeline complet automatisé via `run_all.sh`
- Support Parquet avec compression ZSTD
- Optimisations mémoire agressives (Int32, float32, category)
- Logging colorisé avec timestamps
- Support barres de progression (tqdm)
- Mode rebuild pour panel.parquet

**Scripts principaux:**
- `sas_to_csv.py`: Conversion SAS/TXT optimisée
- `create_mapping.py`: Mapping avec normalisation avancée
- `psid_tool.py`: Grille canonique avec résolution de conflits
- `merge_grid.py`: Fusion de lignes configurables
- `build_final_panel.py`: Panel Parquet memory-efficient
- `build_panel_parent_child.py`: Panel famille-enfant avec relations

**Améliorations:**
- Gestion robuste des erreurs
- Documentation exhaustive
- Copy-on-write pour réduction mémoire
- Partitionnement intelligent par année/module

---

## 📞 Support

Pour toute question technique:
1. Consulter la section [Dépannage](#dépannage)
2. Vérifier les [Exemples d'utilisation](#exemples-dutilisation)
3. Lire les docstrings des scripts (en-tête de chaque .py)

---

**Dernière mise à jour**: 2024-11-01
**Version**: 3.0
**Auteur**: Pipeline PSID RA Team
