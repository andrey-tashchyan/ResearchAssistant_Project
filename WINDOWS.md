# Windows Setup Guide for PSID Pipeline

## Overview

The PSID Pipeline is fully compatible with Windows. This guide provides Windows-specific setup instructions and workarounds.

---

## Quick Start (Windows)

### Option 1: Using PowerShell (Recommended)

```powershell
# Run in PowerShell (right-click Start → Windows PowerShell)
.\setup.ps1
```

If you get an execution policy error:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\setup.ps1
```

### Option 2: Using Batch File

```cmd
# Double-click setup.bat or run in Command Prompt
setup.bat
```

### Option 3: Using Git Bash / WSL

If you have Git Bash or Windows Subsystem for Linux (WSL):
```bash
./setup.sh
```

---

## Differences from Unix/Linux/macOS

### 1. Setup Scripts

| Platform | Script | Activation Command |
|----------|--------|-------------------|
| Unix/macOS/Linux | `setup.sh` | `source psid_env/bin/activate` |
| Windows PowerShell | `setup.ps1` | `.\psid_env\Scripts\Activate.ps1` |
| Windows CMD | `setup.bat` | `psid_env\Scripts\activate.bat` |

### 2. No run_all.sh on Native Windows

The `run_all.sh` script is a Bash script and won't run natively on Windows (without Git Bash/WSL).

**Workaround:** Run scripts individually:

```powershell
# Activate environment first
.\psid_env\Scripts\Activate.ps1

# Then run each step
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
python create_mapping.py --data-dir sorted_data --out-dir out
python psid_tool.py --mapping out/mapping_long.csv --out-dir out --prefer WLTH

# For merge_grid.py (pipe merge_groups.txt manually)
Get-Content merge_groups.txt | python merge_grid.py --file out/canonical_grid.csv --out out/final_grid.csv

python build_final_panel.py --data-dir sorted_data --out-dir out --final-dir final_results
```

### 3. File Paths

Windows uses backslashes (`\`) instead of forward slashes (`/`), but Python handles both automatically.

```python
# Both work in Python on Windows
df = pd.read_parquet('final_results/panel.parquet')
df = pd.read_parquet('final_results\\panel.parquet')
```

### 4. Shell Commands

| Unix/macOS/Linux | Windows (PowerShell) | Windows (CMD) |
|------------------|----------------------|---------------|
| `ls -lh` | `Get-ChildItem` or `dir` | `dir` |
| `rm -rf folder/` | `Remove-Item -Recurse folder\` | `rmdir /s folder\` |
| `cat file.txt` | `Get-Content file.txt` | `type file.txt` |
| `tail -n 10 file.txt` | `Get-Content file.txt -Tail 10` | N/A |

---

## Common Windows Issues and Solutions

### Issue 1: "cannot be loaded because running scripts is disabled"

**Error:**
```
.\setup.ps1 : File cannot be loaded because running scripts is disabled on this system.
```

**Solution:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Issue 2: Python not found

**Error:**
```
'python' is not recognized as an internal or external command
```

**Solution:**
1. Install Python from https://www.python.org/
2. During installation, check **"Add Python to PATH"**
3. Or add Python manually to PATH:
   - Search "Environment Variables" in Windows
   - Edit "Path" in User or System variables
   - Add: `C:\Users\YourName\AppData\Local\Programs\Python\Python3X\`

### Issue 3: Slow installation

**Cause:** Windows Defender scanning downloaded packages

**Solution:**
- Temporarily disable real-time scanning
- Or add Python and pip to exclusions:
  - Windows Security → Virus & threat protection → Manage settings
  - Add exclusions → Folder → Add Python installation folder

### Issue 4: Permission errors

**Error:**
```
PermissionError: [WinError 5] Access is denied
```

**Solution:**
- Run PowerShell as Administrator (right-click → Run as administrator)
- Or change installation directory to user folder

### Issue 5: Long path issues

**Error:**
```
OSError: [WinError 206] The filename or extension is too long
```

**Solution:**
Enable long paths in Windows:
```powershell
# Run as Administrator
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

---

## Windows-Specific Tips

### Use PowerShell ISE or Windows Terminal

For a better experience:
- **Windows Terminal** (recommended): Available in Microsoft Store
- **PowerShell ISE**: Built into Windows

### Virtual Environment Location

```
psid_env/
├── Scripts/           # Windows (different from Unix 'bin/')
│   ├── Activate.ps1   # PowerShell activation
│   ├── activate.bat   # CMD activation
│   ├── python.exe     # Python interpreter
│   └── pip.exe        # Package manager
├── Lib/
└── ...
```

### Performance Optimization

Windows Defender can slow down Python significantly:

1. Add exclusion for Python:
   ```
   C:\Users\YourName\AppData\Local\Programs\Python\
   ```

2. Add exclusion for project:
   ```
   C:\path\to\algo3\
   ```

3. Add exclusion for pip cache:
   ```
   C:\Users\YourName\AppData\Local\pip\
   ```

---

## Alternative: Windows Subsystem for Linux (WSL)

For the best Unix-like experience on Windows, use WSL:

### Setup WSL2

```powershell
# Run in PowerShell as Administrator
wsl --install
```

### Then use Unix commands

```bash
# In WSL terminal
cd /mnt/c/path/to/algo3
./setup.sh
source psid_env/bin/activate
./run_all.sh
```

**Advantages:**
- Full Bash support
- Better performance for some operations
- Native Unix tools

**Disadvantages:**
- File system performance across Windows/Linux boundary
- Additional setup required

---

## Daily Workflow (Windows)

### PowerShell

```powershell
# 1. Open PowerShell in project directory
cd C:\path\to\algo3

# 2. Activate environment
.\psid_env\Scripts\Activate.ps1

# 3. Verify activation (prompt should show (psid_env))
python --version

# 4. Run scripts
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data
python create_mapping.py --data-dir sorted_data --out-dir out
# ... continue with other scripts

# 5. Deactivate when done
deactivate
```

### Command Prompt (CMD)

```cmd
# 1. Open CMD in project directory
cd C:\path\to\algo3

# 2. Activate environment
psid_env\Scripts\activate.bat

# 3. Run scripts (same as PowerShell)
python sas_to_csv.py --file-list file_list.txt --out-dir sorted_data

# 4. Deactivate
deactivate
```

---

## Checking Your Setup

### Verify Installation

```powershell
# Check Python
python --version

# Check pip
pip --version

# Check installed packages
pip list

# Verify critical packages
python -c "import pandas; print(pandas.__version__)"
python -c "import numpy; print(numpy.__version__)"
python -c "import pyarrow; print(pyarrow.__version__)"
```

### Environment Info

```powershell
# Check if virtual environment is active
python -c "import sys; print(sys.prefix)"
# Should show: C:\path\to\algo3\psid_env

# List environment packages
pip list --format=freeze
```

---

## Getting Help

### Windows-Specific Resources

- Python Windows FAQ: https://docs.python.org/3/faq/windows.html
- PowerShell Docs: https://docs.microsoft.com/en-us/powershell/
- WSL Documentation: https://docs.microsoft.com/en-us/windows/wsl/

### Community

- If you encounter Windows-specific issues, check:
  1. Python version compatibility (3.8+ required)
  2. PATH environment variable
  3. Execution policy for PowerShell scripts
  4. Antivirus interference

---

## Summary

| Task | PowerShell Command |
|------|-------------------|
| **Setup** | `.\setup.ps1` |
| **Activate** | `.\psid_env\Scripts\Activate.ps1` |
| **Run script** | `python script.py --args` |
| **Deactivate** | `deactivate` |
| **Check status** | `dir final_results\` |
| **View logs** | `Get-Content -Tail 50 file.log` |

**Key Difference from Unix:** Use `.\script.ps1` instead of `./script.sh` and `\` instead of `/` for paths (though Python accepts both).

---

**Last Updated:** 2024-11-01
**Compatibility:** Windows 10/11, PowerShell 5.1+
