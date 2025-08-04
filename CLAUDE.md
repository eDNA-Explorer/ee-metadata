# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ee-metadata is a high-performance CLI tool for analyzing FASTQ files and generating metadata CSV files for eDNA (environmental DNA) research. The tool supports three operational modes:

1. **FASTQ analysis only** - Scans FASTQ files for primer sequences and pairs reads
2. **FASTQ + metadata integration** - Combines FASTQ analysis with existing metadata CSV
3. **Metadata-only conversion** - Processes metadata without FASTQ files

## Core Architecture

### Main Components

- **`ee_metadata/main.py`** - Single-file CLI application containing all functionality
- **`ee_metadata/primers.csv`** - Database of 50+ primer sequences for various taxa (fish, microbes, fungi, etc.)
- **`pyproject.toml`** - Poetry configuration with Ruff linting/formatting setup

### Key Technical Stack

- **Polars DataFrame library** - High-performance data processing (replaces pandas)
- **Typer + Rich** - CLI framework with beautiful terminal output
- **IUPAC sequence matching** - Handles ambiguous nucleotide codes in primers
- **Fuzzy string matching** - Comprehensive sample name matching across metadata columns

### Data Flow Architecture

1. **Primer Detection**: Scans first N records of each FASTQ file using regex patterns converted from IUPAC codes
2. **Read Pairing**: Matches R1/R2 files based on filename patterns and primer hit analysis
3. **Metadata Integration**: Uses comprehensive fuzzy matching across all string columns to link FASTQ samples with metadata rows
4. **Sample Type Classification**: Rule-based system (positive/negative patterns) to distinguish samples from controls
5. **Output Generation**: Creates standardized metadata CSV with dynamic marker columns

## Development Commands

### Setup
```bash
poetry install                    # Install dependencies
make dev-setup                   # Full development setup (deps + pre-commit)
```

### Code Quality
```bash
make lint                        # Run Ruff linter (check only)
make format                      # Run Ruff formatter  
make fix                         # Auto-fix linting issues + format
make check                       # Run all checks (lint + format check)
```

### Testing and Building
```bash
make test                        # Run pytest
poetry run ee-metadata --help    # Test CLI locally
make build                       # Build package for distribution
```

### Running the Tool
```bash
# Interactive mode (recommended for development)
poetry run ee-metadata

# With parameters
poetry run ee-metadata ./samples/samples --primers ee_metadata/primers.csv --output test.csv

# Tab completion works for paths when installed via uv/pipx
```

## Key Implementation Details

### Primer Detection Algorithm
- Converts IUPAC ambiguity codes to regex patterns (e.g., `R` → `[AG]`, `N` → `[ACGT]`)
- Scans configurable number of records (default: 100) per FASTQ file
- Returns percentage-based hit rates rather than boolean detection
- Prioritizes markers with >15% average hit rate across samples

### Metadata Column Detection
The system uses a sophisticated multi-method approach:
1. **Exact matching** - Direct column name matches (highest priority)
2. **Substring matching** - Partial keyword matches with scoring
3. **Fuzzy matching** - RapidFuzz similarity scoring (threshold: 70%)
4. **Token-based matching** - Searches within column name tokens
5. **Fallback patterns** - Secondary keywords for edge cases

### Sample Matching Strategy
Uses comprehensive fuzzy search across ALL string columns in metadata:
- Normalizes sample names (removes FASTQ suffixes, special chars)
- Scores matches using exact/substring/fuzzy methods
- Aggregates scores across multiple columns per metadata row
- Requires minimum total score of 50 for match acceptance

### Sample Type Classification
Implements intelligent rule detection:
- Analyzes unique values in sample type columns
- Detects positive patterns (`sample`, `project`, `field`) vs negative patterns (`control`, `blank`, `reference`)
- Supports user-confirmed rules (e.g., `"project"` or `"!control"`)
- Falls back to default logic for unspecified rules

## File Organization

- **`docs/`** - Documentation (moved from root except README)
- **`samples/`** - Example FASTQ files for testing
- **`ee_metadata/`** - Source code package
- **`.pre-commit-config.yaml`** - Automated code quality hooks
- **`Makefile`** - Development convenience commands

## Important Notes

- The main application is intentionally kept as a single file (`main.py`) for simplicity
- Ruff configuration in `pyproject.toml` includes 15+ rule categories for comprehensive code quality
- Pre-commit hooks automatically run Ruff linting/formatting plus security checks
- The tool handles large datasets efficiently using Polars rather than pandas
- Interactive prompts guide users through complex metadata mapping scenarios
- All coordinate and date normalization happens automatically during processing

## Testing with Sample Data

The `samples/samples/` directory contains real FASTQ files for testing:
```bash
poetry run ee-metadata ./samples/samples --output test_output.csv
```

This will trigger the full analysis pipeline including primer detection, read pairing, and metadata generation.