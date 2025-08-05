# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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