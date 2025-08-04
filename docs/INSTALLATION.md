# Installation Guide for ee-metadata

## Quick Installation Options

### 1. uv (Fastest - Recommended)
[uv](https://github.com/astral-sh/uv) is the fastest Python package installer:

```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install ee-metadata
uv tool install ee-metadata
```

### 2. pipx (Isolated Installation)
[pipx](https://pypa.github.io/pipx/) installs CLI tools in isolated environments:

```bash
# Install pipx if you don't have it
python -m pip install --user pipx
python -m pipx ensurepath

# Install ee-metadata
pipx install ee-metadata
```

### 3. pip (Traditional)
Standard pip installation:

```bash
pip install ee-metadata
```

## Verification

After installation, verify it works:

```bash
# Check version
ee-metadata --version

# View help
ee-metadata --help

# Test with sample data (if available)
ee-metadata /path/to/fastq/files
```

## Installation from Source

For development or latest features:

```bash
# Clone repository
git clone https://github.com/eDNA-Explorer/ee-metadata.git
cd ee-metadata

# Install with Poetry
poetry install

# Run with Poetry
poetry run ee-metadata --help
```

## Shell Completion

Enable tab completion for better UX:

```bash
# For bash
ee-metadata --install-completion bash

# For zsh  
ee-metadata --install-completion zsh

# For fish
ee-metadata --install-completion fish
```

## Troubleshooting

### Permission Issues
If you get permission errors:
```bash
# For pip, use --user flag
pip install --user ee-metadata

# Or use virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install ee-metadata
```

### Python Version
ee-metadata requires Python 3.8 or higher:
```bash
python --version  # Should be 3.8+
```

### Dependencies
All dependencies are installed automatically:
- polars (fast DataFrame processing)
- typer (CLI framework)
- rich (terminal formatting)
- python-dateutil (date parsing)
- rapidfuzz (fuzzy string matching)

## Updating

### With uv
```bash
uv tool upgrade ee-metadata
```

### With pipx
```bash
pipx upgrade ee-metadata
```

### With pip
```bash
pip install --upgrade ee-metadata
```

## Uninstalling

### With uv
```bash
uv tool uninstall ee-metadata
```

### With pipx
```bash
pipx uninstall ee-metadata
```

### With pip
```bash
pip uninstall ee-metadata
```