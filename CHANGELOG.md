# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2025-08-04

### Added
- Release 0.2.2

### Changed
- Updated dependencies to latest versions

### Fixed
- Various bug fixes and improvements

### Commits since last release:
- 2a6e328 Fix PyPI publishing to use OIDC trusted publishing instead of API tokens


## [0.2.1] - 2025-08-04

### Added
- Release 0.2.1

### Changed
- Updated dependencies to latest versions

### Fixed
- Various bug fixes and improvements

### Commits since last release:
- e866ff3 Add comprehensive release automation with version bumping and changelog
- 8b73a02 Upgrade to Rich 14.1.0 for latest terminal output features
- 44a8f96 Upgrade to Typer 0.16.0 for improved stability and compatibility
- e286364 Enhance GitHub Actions security with explicit permissions and pinned actions
- c21d136 Fix Typer/Click compatibility issue in CLI
- d59f85d Update Python version requirements to 3.9+ for Poetry compatibility
- 5df95c7 Add comprehensive CLAUDE.md for future Claude Code instances
- 7ca740c Configure Claude Code project hooks for automated code quality
- f82004e Set up GitHub Actions CI/CD pipeline
- 5a8d5a2 Add publishing automation script
- cbeea12 Add Makefile with comprehensive development commands
- 5304fe7 Configure pre-commit hooks for automated code quality
- ab01150 Add comprehensive .gitignore for Python and development files
- b4c269c Organize project documentation into docs directory
- 892a276 Implement core FASTQ analysis and metadata generation engine
- 0de512d Add comprehensive README with installation and usage guide
- b76644b Initial project setup with Poetry and dependencies
- 454b5f1 Initial commit


## [Unreleased]

### Added
- Automated release script with version bumping and changelog generation
- Comprehensive Makefile with release management commands

### Changed
- Upgraded to Typer 0.16.0 for improved CLI stability
- Upgraded to Rich 14.1.0 for latest terminal output features
- Enhanced GitHub Actions security with pinned actions and explicit permissions

### Fixed
- Resolved Typer/Click compatibility issues causing CLI help crashes
- Fixed GitHub Actions Python version compatibility for Poetry requirements

## [0.2.0] - Initial Release

### Added
- Complete FASTQ analysis and metadata generation engine
- Support for three operational modes: FASTQ-only, FASTQ+metadata, metadata-only
- Comprehensive primer detection with IUPAC sequence matching
- Intelligent metadata column detection with fuzzy matching
- Sample type classification with rule-based system
- Beautiful CLI interface with Rich terminal output and progress bars
- Comprehensive development workflow with Ruff, pre-commit, and CI/CD
- Claude Code integration with project hooks for automatic code quality
- Distribution-ready package configuration for uv/pipx installation