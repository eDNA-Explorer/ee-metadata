#!/bin/bash

# ee-metadata Release Script
# Automates version bumping, changelog generation, and GitHub release creation
# Usage: ./scripts/release.sh [patch|minor|major]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [[ ! -f "pyproject.toml" ]]; then
    print_error "Must be run from project root directory"
    exit 1
fi

# Check for required tools
command -v poetry >/dev/null 2>&1 || { print_error "Poetry is required but not installed"; exit 1; }
command -v gh >/dev/null 2>&1 || { print_error "GitHub CLI is required but not installed"; exit 1; }

# Get version bump type
BUMP_TYPE=${1:-patch}

if [[ ! "$BUMP_TYPE" =~ ^(patch|minor|major)$ ]]; then
    print_error "Invalid version bump type. Use: patch, minor, or major"
    exit 1
fi

print_status "Starting release process with $BUMP_TYPE version bump..."

# Ensure we're on main branch and up to date
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    print_error "Must be on main branch to create release"
    exit 1
fi

print_status "Pulling latest changes..."
git pull origin main

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    print_error "You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# Get current version
CURRENT_VERSION=$(poetry version -s)
print_status "Current version: $CURRENT_VERSION"

# Bump version using Poetry
print_status "Bumping version ($BUMP_TYPE)..."
poetry version $BUMP_TYPE
NEW_VERSION=$(poetry version -s)
print_success "New version: $NEW_VERSION"

# Update version in __init__.py
print_status "Updating version in __init__.py..."
sed -i.bak "s/__version__ = \".*\"/__version__ = \"$NEW_VERSION\"/" ee_metadata/__init__.py
rm ee_metadata/__init__.py.bak

# Generate changelog entry
CHANGELOG_FILE="CHANGELOG.md"
RELEASE_DATE=$(date +"%Y-%m-%d")

print_status "Generating changelog entry..."

# Create changelog if it doesn't exist
if [[ ! -f "$CHANGELOG_FILE" ]]; then
    cat > "$CHANGELOG_FILE" << 'EOF'
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

EOF
fi

# Get commits since last tag for changelog
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [[ -n "$LAST_TAG" ]]; then
    COMMITS=$(git log ${LAST_TAG}..HEAD --oneline --no-merges)
else
    COMMITS=$(git log --oneline --no-merges)
fi

# Generate changelog entry
TEMP_CHANGELOG=$(mktemp)
cat > "$TEMP_CHANGELOG" << EOF
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [${NEW_VERSION}] - ${RELEASE_DATE}

### Added
- Release ${NEW_VERSION}

### Changed
- Updated dependencies to latest versions

### Fixed
- Various bug fixes and improvements

### Commits since last release:
EOF

# Add commits to changelog
if [[ -n "$COMMITS" ]]; then
    echo "$COMMITS" | while read -r commit; do
        echo "- $commit" >> "$TEMP_CHANGELOG"
    done
else
    echo "- Initial release" >> "$TEMP_CHANGELOG"
fi

echo "" >> "$TEMP_CHANGELOG"

# Append existing changelog content (skip header)
if [[ -f "$CHANGELOG_FILE" ]]; then
    tail -n +7 "$CHANGELOG_FILE" >> "$TEMP_CHANGELOG"
fi

mv "$TEMP_CHANGELOG" "$CHANGELOG_FILE"

print_success "Updated $CHANGELOG_FILE"

# Commit changes
print_status "Committing version bump and changelog..."
git add pyproject.toml ee_metadata/__init__.py "$CHANGELOG_FILE"
git commit -m "$(cat <<EOF
Release v${NEW_VERSION}

- Bump version from ${CURRENT_VERSION} to ${NEW_VERSION}
- Update changelog with release notes
- Prepare for PyPI release

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"

# Create and push tag
TAG_NAME="v${NEW_VERSION}"
print_status "Creating tag: $TAG_NAME"
git tag -a "$TAG_NAME" -m "Release v${NEW_VERSION}"

print_status "Pushing changes and tag..."
git push origin main
git push origin "$TAG_NAME"

# Generate release notes
RELEASE_NOTES=$(mktemp)
cat > "$RELEASE_NOTES" << EOF
## 🎉 Release v${NEW_VERSION}

### What's New
$(if [[ -n "$COMMITS" ]]; then echo "$COMMITS" | head -5 | sed 's/^/- /'; else echo "- Initial release of ee-metadata CLI tool"; fi)

### Installation
\`\`\`bash
# Install with uv (recommended)
uv tool install ee-metadata

# Install with pipx
pipx install ee-metadata

# Install with pip
pip install ee-metadata
\`\`\`

### Usage
\`\`\`bash
# Interactive mode
ee-metadata

# With parameters
ee-metadata ./fastq-files --primers primers.csv --output metadata.csv
\`\`\`

### Full Changelog
See [CHANGELOG.md](./CHANGELOG.md) for complete details.

---
**Full Changelog**: https://github.com/eDNA-Explorer/ee-metadata/compare/${LAST_TAG}...${TAG_NAME}
EOF

# Create GitHub release
print_status "Creating GitHub release..."
gh release create "$TAG_NAME" \
    --title "Release v${NEW_VERSION}" \
    --notes-file "$RELEASE_NOTES" \
    --latest

# Clean up
rm "$RELEASE_NOTES"

print_success "🎉 Release v${NEW_VERSION} created successfully!"
print_status "GitHub Actions will now automatically publish to PyPI"
print_status "Monitor the progress at: https://github.com/eDNA-Explorer/ee-metadata/actions"

echo ""
print_status "Release summary:"
echo "  📦 Version: $CURRENT_VERSION → $NEW_VERSION"
echo "  🏷️  Tag: $TAG_NAME"
echo "  📋 Changelog: Updated"
echo "  🚀 GitHub Release: Created"
echo "  ⚡ PyPI Publish: Triggered"

print_success "Release process completed!"