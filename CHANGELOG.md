# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-03-04

### Added
- Release 0.2.3

### Changed
- Updated dependencies to latest versions

### Fixed
- Various bug fixes and improvements

### Commits since last release:
- fae5f4d Update readme with new commands
- c53f774 Display admin status and tag in CLI commands
- d7601ea Add role field to UserInfo and validate_token
- 515ac33 Fix linting issues
- ef1ecda Update main.py to import from shared files
- c3f9763 Extract upload functions into shared file
- 32f542e Extract auth functions into shared file
- 433defa Init commands module
- 4cf2a82 Extract CLI functions into shared cli.py
- 1c3b86e Replace os.exit(1) with Pythons atexit
- b57e66c Add test for token refresh with file storage
- ddce2ad Preserve file based storage in ensure_valid_token() fn
- 6495af1 Prevent html dump on terminal logs
- 26d88df Add keyringg logging
- 1ba7b76 Rm dual lock files
- 5d367bf null safety for ensure_valid_token fn
- 34e03ed Add useforsecuriy flags
- eeca9df Run tests in CI
- 31e1873 Add tests for resumability feature
- 06dfa95 Allow byte-level resumable file uploadas
- 816a0fc Fix buggy ctrl+c by using
- 7e10957 Allow user to ctrl+c to exit upload
- 54b849a Remove legacy ~/.ednaexplorer/token.json support
- f6272e6 Add TestIsTokenExpiringSoon and TestRefreshAccessToken
- ad6e811 Add ensure_valid_token() fn to make sure token is valid or to refresh
- 8f3309a Add helper functions for automatic refreshing tokens
- a23b437 Add ACCOUNT_REFRESH_TOKEN and refresh_token field for automatic refreshes
- c4f90f2 Update tests with ConfigureCryptFile
- 7e83fcf Add cryptfile dep
- 8034b6e Test token helper functions
- f0d0625 Test keyring impl
- 5e592dd load/store token helper functions
- 2129ef5 Store token using keyring, add explicity fallback to plaintext with --insecure-storage flag
- 1954220 Rm token path/save from auth file
- 69ada64 Add keyring dep
- 52d80fd Warn when using non-default API URL during login
- de38302 Verify callback state before exchanging authorization code
- 202126c Document decode_token_claims as UX guard, not security boundary
- 1375cd7 Warn instead of silently suppressing chmod failure on token file
- 15aef0f Add device code flow to CLI login for headless environments
- b5e07cc Wire up cancel_event, REUPLOAD display, and updated summary in CLI
- 0058eb8 Add circuit breaker to upload_file for token expiry
- c0ebeee Add _retry_transient helper with exponential backoff
- 87ec139 Add REUPLOAD handling in match_local_files
- 12675e0 Add TokenExpiredUploadError, note_type/skipped fields, and noteType parsing
- 6811987 Add CLI upload command with file matching and concurrent uploads
- 3a0f6ca Set token directory permissions to 0o700
- 8d3e83f Exchange code for JWT token
- 40f0a3c Add exchange_code() fn
- 1ffd59d Add --no-project flag in uv install CI
- 9ef64b3 Update CI to test with uv instead of poetry
- 83a79d4 Setup browser automatic callback
- 70ae04b Setup browser automatic callback during auth
- 6560927 Reformat toml file to work with uv
- 585a48d Update text
- 0764514 Rm python 3.9 support
- 5cc5aed Add auth CLI commands
- 3bdcf20 Add auth functions
- 1809579 Add httpx dep
- 9c01fac Add animated gif to README.md


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