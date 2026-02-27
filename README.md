# ee-metadata

A command-line tool from [eDNA Explorer](https://www.ednaexplorer.org) for analyzing FASTQ files, generating metadata CSVs, and uploading data — all from your terminal.

![cleanshot_2025-08-04_at_14 58 10](https://github.com/user-attachments/assets/27709a1b-3873-4e08-bc84-23d667295220)

## Installation

### Using uv (Recommended)

[uv](https://docs.astral.sh/uv/) installs `ee-metadata` as a standalone tool — no need to manage Python environments yourself.

```bash
uv tool install ee-metadata
```

> Don't have uv? Install it with `curl -LsSf https://astral.sh/uv/install.sh | sh` (macOS/Linux) or see the [uv install guide](https://docs.astral.sh/uv/getting-started/installation/).

### Using pip

```bash
pip install ee-metadata
```

### Verify it works

```bash
ee-metadata --help
```

## Quick Start

The typical workflow looks like this:

```bash
# 1. Log in to eDNA Explorer
ee-metadata login

# 2. Generate a metadata CSV from your FASTQ files
ee-metadata generate ./my-fastq-files --output metadata.csv

# 3. Upload files to your project
ee-metadata upload ./my-fastq-files --project YOUR_PROJECT_ID
```

## Commands

### `login` — Authenticate with eDNA Explorer

Logs you in so you can upload files. By default, it opens your browser to complete the login.

```bash
ee-metadata login [OPTIONS]
```

| Option | Description |
|---|---|
| `--no-browser` | Skip the browser and paste a token manually |
| `--device` | Use device-code flow (useful over SSH or on servers) |
| `--insecure-storage` | Store token as a plain text file instead of your system keyring |
| `--api-url`, `-u` | Custom API URL (defaults to `https://www.ednaexplorer.org`) |

Your token is stored securely in your system's keyring (macOS Keychain, Windows Credential Manager, or Linux Secret Service). If no keyring is available, you'll be prompted to use `--insecure-storage`.

### `logout` — Remove stored credentials

```bash
ee-metadata logout
```

### `auth-status` — Check your login status

Shows whether you're logged in, which account is active, and how your token is stored.

```bash
ee-metadata auth-status
```

### `generate` — Analyze FASTQ files and create metadata

Scans `.fastq.gz` files for primer sequences, pairs forward/reverse reads, and outputs a metadata CSV.

```bash
ee-metadata generate [INPUT_DIR] [OPTIONS]
```

| Option | Short | Description | Default |
|---|---|---|---|
| `--primers` | `-p` | Path to a primers CSV file | Built-in primer database |
| `--input-metadata` | `-m` | Existing metadata CSV to merge with | — |
| `--output` | `-o` | Output CSV filename | `metadata.csv` |
| `--num-records` | `-n` | FASTQ records to scan per file | `100` |
| `--force-pairing` | | Force R1/R2 pairing by filename | `false` |

**Examples:**

```bash
# Interactive mode — the tool will prompt you for what it needs
ee-metadata generate

# Specify a directory of FASTQ files
ee-metadata generate ./data/raw_reads

# Merge with an existing metadata spreadsheet
ee-metadata generate ./data/raw_reads --input-metadata sample_sheet.csv --output merged.csv
```

### `upload` — Upload FASTQ files to a project

Uploads `.fastq.gz` files to an eDNA Explorer project. Requires being logged in first.

```bash
ee-metadata upload DIRECTORY --project PROJECT_ID [OPTIONS]
```

| Option | Short | Description | Default |
|---|---|---|---|
| `--project` | `-p` | Project ID to upload to (required) | — |
| `--dry-run` | | Preview what would be uploaded without uploading | `false` |
| `--concurrency` | `-c` | Number of parallel uploads (1–8) | `4` |

**Features:**
- Resumable uploads — if a large file transfer gets interrupted, it picks up where it left off
- Skips files that have already been uploaded and verified
- Shows progress bars with transfer speed and ETA

**Examples:**

```bash
# Preview an upload plan without sending anything
ee-metadata upload ./my-fastq-files --project abc123 --dry-run

# Upload with 2 parallel connections (slower internet)
ee-metadata upload ./my-fastq-files --project abc123 --concurrency 2
```

## Shell Tab Completion (Optional)

Enable tab completion for file paths and options:

```bash
# Bash
ee-metadata --install-completion bash

# Zsh
ee-metadata --install-completion zsh

# Fish
ee-metadata --install-completion fish
```

## Development Setup

```bash
git clone https://github.com/eDNA-Explorer/ee-metadata.git
cd ee-metadata
uv sync
```

Run locally during development:

```bash
uv run ee-metadata --help
```

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request on [GitHub](https://github.com/eDNA-Explorer/ee-metadata).
