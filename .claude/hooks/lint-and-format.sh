#!/bin/bash

# Claude Code hook script for ee-metadata project
# Runs Ruff linting and formatting after file edits

set -e  # Exit on any error

# Change to project directory
cd "$CLAUDE_PROJECT_DIR"

# Check if poetry is available
if ! command -v poetry &> /dev/null; then
    echo "⚠️  Poetry not found - skipping lint and format"
    exit 0
fi

# Check if this is a Python file edit
if [[ "$CLAUDE_TOOL_CALL" =~ (Write|Edit|MultiEdit) ]] && [[ "$CLAUDE_TOOL_ARGS" =~ \.py ]]; then
    echo "🔧 Running Ruff linting and formatting after Python file edit..."
    
    # Run Ruff with auto-fix and formatting
    if poetry run ruff check --fix . 2>/dev/null; then
        echo "✅ Ruff linting completed"
    else
        echo "⚠️  Ruff linting found issues that couldn't be auto-fixed"
    fi
    
    if poetry run ruff format . 2>/dev/null; then
        echo "✅ Ruff formatting completed"
    else
        echo "⚠️  Ruff formatting failed"
    fi
    
    echo "📋 Code quality check complete"
else
    # For non-Python files, just check if pyproject.toml was modified
    if [[ "$CLAUDE_TOOL_ARGS" =~ pyproject\.toml ]]; then
        echo "🔧 pyproject.toml modified - checking configuration..."
        if poetry check 2>/dev/null; then
            echo "✅ Poetry configuration is valid"
        else
            echo "⚠️  Poetry configuration may have issues"
        fi
    fi
fi