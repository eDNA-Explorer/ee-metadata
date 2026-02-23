"""Shared CLI singletons for ee-metadata.

Centralizes the Typer app and Rich console so that command modules can
register commands without circular imports.
"""

from pathlib import Path

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
    p = Path(incomplete).expanduser()

    # Get matching paths
    if p.is_dir():
        matches = [str(m) for m in p.glob("*")]
    else:
        matches = [str(m) for m in p.parent.glob(f"{p.name}*")]

    return matches
