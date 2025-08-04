#!/bin/bash

# Script to build and publish ee-metadata to PyPI
# Usage: ./scripts/publish.sh [test|prod]

set -e

ENVIRONMENT=${1:-test}

echo "🔧 Building ee-metadata package..."

# Clean previous builds
rm -rf dist/
rm -rf build/

# Build the package
poetry build

echo "✅ Build complete! Generated files:"
ls -la dist/

if [ "$ENVIRONMENT" = "test" ]; then
    echo "🧪 Publishing to TestPyPI..."
    poetry publish --repository testpypi
    echo "📦 Published to TestPyPI! Install with:"
    echo "pip install --index-url https://test.pypi.org/simple/ ee-metadata"
elif [ "$ENVIRONMENT" = "prod" ]; then
    echo "🚀 Publishing to PyPI..."
    read -p "Are you sure you want to publish to production PyPI? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        poetry publish
        echo "🎉 Published to PyPI! Install with:"
        echo "pip install ee-metadata"
        echo "uv tool install ee-metadata"
        echo "pipx install ee-metadata"
    else
        echo "❌ Cancelled publication"
        exit 1
    fi
else
    echo "❓ Unknown environment: $ENVIRONMENT"
    echo "Usage: $0 [test|prod]"
    exit 1
fi