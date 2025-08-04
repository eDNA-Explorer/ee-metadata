# Packaging Guide for ee-metadata

## Current Status ✅

The `ee-metadata` package is now fully configured for distribution via PyPI and can be installed using:

- **uv**: `uv tool install ee-metadata`
- **pipx**: `pipx install ee-metadata` 
- **pip**: `pip install ee-metadata`

## Package Structure

```
ee-metadata/
├── ee_metadata/
│   ├── __init__.py          # Package metadata
│   ├── main.py             # Main CLI application
│   └── primers.csv         # Default primer database
├── pyproject.toml          # Package configuration
├── README.md               # Main documentation
├── INSTALLATION.md         # Installation guide
├── scripts/publish.sh      # Publishing script
└── .github/workflows/      # CI/CD automation
    ├── test.yml           # Testing workflow
    └── publish.yml        # Publishing workflow
```

## Build & Test

```bash
# Build the package
poetry build

# Test the build
pip install dist/ee_metadata-*.whl
ee-metadata --help
```

## Publishing Steps

### 1. Prepare Release
```bash
# Update version in pyproject.toml
# Update CHANGELOG if needed
# Commit changes
```

### 2. Test Publication
```bash
./scripts/publish.sh test
```

### 3. Production Publication
```bash
./scripts/publish.sh prod
```

## GitHub Actions

The repository includes automated workflows:

- **test.yml**: Runs on every push/PR
  - Tests on multiple OS (Ubuntu, macOS, Windows)
  - Tests Python 3.8-3.12
  - Validates package builds

- **publish.yml**: Publishes on release
  - Automatic PyPI publication on GitHub releases
  - Manual TestPyPI publication via workflow dispatch

## Required Secrets

For GitHub Actions publishing, set these repository secrets:
- `PYPI_API_TOKEN`: PyPI API token for production
- `TEST_PYPI_API_TOKEN`: TestPyPI API token for testing

## Package Features

✅ **Multi-platform support**: Windows, macOS, Linux  
✅ **Python 3.8+ compatibility**  
✅ **Isolated installation** with uv/pipx  
✅ **Shell completion** support  
✅ **Rich terminal output**  
✅ **Comprehensive documentation**  
✅ **CI/CD automation**  
✅ **Proper dependency management**  

## Installation Examples

After publishing to PyPI, users can install with:

```bash
# Fastest (uv)
uv tool install ee-metadata

# Isolated (pipx)  
pipx install ee-metadata

# Traditional (pip)
pip install ee-metadata

# From source
git clone https://github.com/eDNA-Explorer/ee-metadata.git
cd ee-metadata
poetry install
```

## Next Steps

1. **Test thoroughly** with the built package
2. **Update version** in `pyproject.toml` when ready
3. **Create GitHub release** to trigger automatic PyPI publication
4. **Announce** the release to users

The package is ready for distribution! 🚀