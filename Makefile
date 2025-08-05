# Makefile for ee-metadata development tasks
# =============================================================================

.PHONY: help install lint format check test build clean dev-setup

# Default target
help:
	@echo "ee-metadata Development Commands"
	@echo "==============================="
	@echo ""
	@echo "Setup:"
	@echo "  install     Install project dependencies"
	@echo "  dev-setup   Setup development environment (install deps + pre-commit)"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint        Run ruff linter (check only)"
	@echo "  format      Run ruff formatter (fix formatting)"
	@echo "  check       Run all checks (lint + format check)"
	@echo "  fix         Run linter and formatter with auto-fix"
	@echo ""
	@echo "Testing:"
	@echo "  test        Run tests with pytest"
	@echo "  test-cov    Run tests with coverage report"
	@echo ""
	@echo "Building:"
	@echo "  build       Build package for distribution"
	@echo "  clean       Clean build artifacts"
	@echo ""
	@echo "Pre-commit:"
	@echo "  pre-commit-install   Install pre-commit hooks"
	@echo "  pre-commit-run       Run pre-commit on all files"
	@echo ""
	@echo "Release:"
	@echo "  release              Create patch release (default)"
	@echo "  release-patch        Create patch release (0.0.X)"
	@echo "  release-minor        Create minor release (0.X.0)"
	@echo "  release-major        Create major release (X.0.0)"

# Installation and setup
install:
	poetry install

dev-setup: install pre-commit-install
	@echo "✅ Development environment setup complete!"

# Linting and formatting
lint:
	poetry run ruff check .

format:
	poetry run ruff format .

check: lint
	poetry run ruff format --check .

fix:
	poetry run ruff check --fix .
	poetry run ruff format .

# Testing
test:
	poetry run pytest

test-cov:
	poetry run pytest --cov=ee_metadata --cov-report=html --cov-report=term

# Building
build: clean
	poetry build

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Pre-commit
pre-commit-install:
	poetry run pre-commit install

pre-commit-run:
	poetry run pre-commit run --all-files

# CI simulation
ci: check test
	@echo "✅ CI checks passed!"

# Quick development cycle
dev: fix test
	@echo "✅ Development cycle complete!"

# Release management
release-patch:
	@echo "Creating patch release..."
	@./scripts/release.sh patch

release-minor:
	@echo "Creating minor release..."
	@./scripts/release.sh minor

release-major:
	@echo "Creating major release..."
	@./scripts/release.sh major

release: release-patch