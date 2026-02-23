"""Shared CLI singletons for ee-metadata.

Centralizes the Typer app and Rich console so that command modules can
register commands without circular imports.
"""

import glob
import os

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


def complete_path(incomplete: str):
    """Custom path completion function."""
    # Handle empty input
    if not incomplete:
        incomplete = "./"

    # Expand user home directory
    incomplete = os.path.expanduser(incomplete)

    # Get matching paths
    if os.path.isdir(incomplete):
        matches = glob.glob(os.path.join(incomplete, "*"))
    else:
        matches = glob.glob(incomplete + "*")

    return matches
