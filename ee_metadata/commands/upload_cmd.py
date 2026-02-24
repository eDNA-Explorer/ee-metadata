"""Upload command for ee-metadata CLI."""

import concurrent.futures.thread as _cft
import os
import signal
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event

import typer
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from ee_metadata.auth import AuthError, TokenExpiredError, validate_token
from ee_metadata.cli import app, complete_path, console
from ee_metadata.commands.auth_cmd import ensure_valid_token
from ee_metadata.token_storage import get_token
from ee_metadata.upload import (
    UploadError,
    get_allowed_filenames,
    match_local_files,
    upload_file,
)


def _format_size(size_bytes: int) -> str:
    """Format a byte count as a human-readable string."""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / 1024:.1f} KB"


@app.command()
def upload(
    directory: Path = typer.Argument(
        ...,
        help="Directory containing FASTQ files to upload.",
        autocompletion=complete_path,
    ),
    project: str = typer.Option(
        ...,
        "--project",
        "-p",
        help="Project ID to upload files to.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be uploaded without actually uploading.",
    ),
    concurrency: int = typer.Option(
        4,
        "--concurrency",
        "-c",
        min=1,
        max=8,
        help="Number of concurrent uploads (1-8).",
    ),
):
    """Upload FASTQ files to eDNA Explorer.

    Requires authentication. Run 'ee-metadata login' first.
    """
    # Validate directory exists
    if not directory.exists():
        console.print(f"[bold red]Error:[/bold red] Directory not found: {directory}")
        raise typer.Exit(code=1)

    if not directory.is_dir():
        console.print(f"[bold red]Error:[/bold red] Not a directory: {directory}")
        raise typer.Exit(code=1)

    # Load and validate token
    token_data = get_token()
    if token_data is None:
        console.print(
            "[bold red]Error:[/bold red] Not logged in. "
            "Run 'ee-metadata login' to authenticate."
        )
        raise typer.Exit(code=1)

    # Refresh token transparently if expiring soon
    token_data = ensure_valid_token(token_data)

    # Validate token is still valid
    console.print("[dim]Checking authentication...[/dim]")

    try:
        user = validate_token(token_data.token, token_data.api_url)
    except TokenExpiredError:
        console.print(
            "[bold red]Error:[/bold red] Your session has expired. "
            "Run 'ee-metadata login' again."
        )
        raise typer.Exit(code=1) from None
    except AuthError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from None

    # Show auth status
    display_name = user.name or user.email
    if user.role == "ADMIN":
        console.print(
            f"[bold green]✓ Authenticated as {display_name}[/bold green] "
            "[bold cyan](Admin)[/bold cyan]\n"
        )
    else:
        console.print(f"[bold green]✓ Authenticated as {display_name}[/bold green]\n")

    # Scan for FASTQ files
    files = sorted(directory.glob("*.fastq.gz"))
    if not files:
        console.print(
            f"[bold yellow]Warning:[/bold yellow] No .fastq.gz files found in "
            f"{directory}"
        )
        raise typer.Exit(code=0)

    # Fetch allowed filenames from server
    console.print("[dim]Fetching project file list...[/dim]")
    try:
        project_info = get_allowed_filenames(
            project, token_data.token, token_data.api_url
        )
    except (UploadError, AuthError) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from None

    # Match local files against server list
    match_result = match_local_files(files, project_info.allowed_files)

    # Build upload queue: new files + files that need re-upload
    upload_queue = match_result.matched + match_result.needs_reupload

    # Display upload plan table
    table = Table(title="Upload Plan")
    table.add_column("File", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Size")

    for local_path, _af in match_result.matched:
        size = _format_size(local_path.stat().st_size)
        table.add_row(local_path.name, "[green]Ready to upload[/green]", size)

    for local_path, _af in match_result.needs_reupload:
        size = _format_size(local_path.stat().st_size)
        table.add_row(
            local_path.name, "[yellow]Re-upload (verify failed)[/yellow]", size
        )

    for local_path, _af in match_result.already_uploaded:
        size = _format_size(local_path.stat().st_size)
        table.add_row(local_path.name, "[dim]Already uploaded[/dim]", size)

    for local_path in match_result.unmatched_local:
        size = _format_size(local_path.stat().st_size)
        table.add_row(local_path.name, "[red]Not in project[/red]", size)

    for af in match_result.unmatched_server:
        table.add_row(
            af.file_name or af.normalized_name, "[dim]Missing locally[/dim]", "-"
        )

    console.print(table)

    if not upload_queue:
        console.print("\n[yellow]No new files to upload.[/yellow]")
        raise typer.Exit(code=0)

    # Summary line
    upload_size = sum(p.stat().st_size for p, _ in upload_queue)
    console.print(
        f"\n[bold]{len(upload_queue)} file(s)[/bold] to upload "
        f"({_format_size(upload_size)})"
    )

    # Dry-run exits here
    if dry_run:
        console.print("[dim]Dry run — nothing was uploaded.[/dim]")
        raise typer.Exit(code=0)

    # Confirm upload
    if not typer.confirm("Proceed with upload?"):
        console.print("[dim]Upload cancelled.[/dim]")
        raise typer.Exit(code=0)

    # Concurrent upload with Rich Progress
    results: list = []
    cancel_event = Event()
    interrupted = False

    # ------------------------------------------------------------------
    # SIGINT handler — the default KeyboardInterrupt mechanism is
    # unreliable while the main thread is blocked inside C-level lock
    # waits (as_completed / threading.Event.wait).  A custom handler
    # gives us immediate, deterministic cancellation.
    #   * 1st Ctrl+C -> set cancel_event so worker threads stop after
    #     their current chunk, then break out of the polling loop.
    #   * 2nd Ctrl+C -> os._exit(1) for an instant kill.
    # ------------------------------------------------------------------
    _original_sigint = signal.getsignal(signal.SIGINT)

    def _sigint_handler(_signum, _frame):
        nonlocal interrupted
        cancel_event.set()
        if interrupted:
            # Second press — force-exit immediately.
            os._exit(1)
        interrupted = True

    signal.signal(signal.SIGINT, _sigint_handler)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console,
    )
    progress.start()
    executor = ThreadPoolExecutor(max_workers=concurrency)

    try:
        # Overall progress bar
        overall_task = progress.add_task("Overall", total=upload_size)

        # Map futures to their progress task IDs
        future_to_info: dict = {}

        for local_path, af in upload_queue:
            file_size = local_path.stat().st_size
            task_id = progress.add_task(local_path.name, total=file_size)

            def _make_callback(tid, overall_tid):
                def _cb(bytes_uploaded: int):
                    progress.advance(tid, bytes_uploaded)
                    progress.advance(overall_tid, bytes_uploaded)

                return _cb

            future = executor.submit(
                upload_file,
                filepath=local_path,
                allowed_file=af,
                project_id=project,
                project_metadata_id=project_info.project_metadata_id,
                token=token_data.token,
                api_url=token_data.api_url,
                progress_callback=_make_callback(task_id, overall_task),
                cancel_event=cancel_event,
            )
            future_to_info[future] = (local_path.name, task_id)

        # Poll for completed futures with a short timeout so the main
        # thread wakes up frequently and can react to cancel_event
        # (set by the SIGINT handler) without waiting for a future to
        # finish.
        pending = set(future_to_info)
        while pending and not interrupted:
            done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
            for future in done:
                result = future.result()
                results.append(result)
                _name, tid = future_to_info[future]
                if result.success:
                    progress.update(tid, description=f"[green]✓[/green] {_name}")
                elif result.skipped:
                    progress.update(tid, description=f"[yellow]—[/yellow] {_name}")
                else:
                    progress.update(tid, description=f"[red]✗[/red] {_name}")
    except KeyboardInterrupt:
        interrupted = True
        cancel_event.set()
    finally:
        progress.stop()
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)
        signal.signal(signal.SIGINT, _original_sigint)
        if interrupted:
            # Prevent the process from hanging at exit.  Python's atexit
            # handler calls executor.shutdown(wait=True) on every pool
            # tracked in concurrent.futures.thread._threads_queues.
            # Workers may still be blocked on HTTP I/O, so we remove
            # this pool's threads from the global registry.
            for t in list(_cft._threads_queues):
                _cft._threads_queues.pop(t, None)

            console.print("\n[bold yellow]Upload cancelled by user.[/bold yellow]")
            console.print(
                "\nRe-run this command to upload remaining files. "
                "Already-uploaded files will be skipped automatically.\n"
            )
            raise typer.Exit(code=130)

    # Summary
    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success and not r.skipped)
    skipped = sum(1 for r in results if r.skipped)

    console.print()
    if succeeded:
        console.print(
            f"[bold green]✓ {succeeded} file(s) uploaded successfully[/bold green]"
        )
    if skipped:
        console.print(
            f"[bold yellow]— {skipped} file(s) skipped (token expired)[/bold yellow]"
        )
    if failed:
        console.print(f"[bold red]✗ {failed} file(s) failed:[/bold red]")
        for r in results:
            if not r.success and not r.skipped:
                console.print(f"  {r.filename}: {r.error}")

    if cancel_event.is_set():
        console.print(
            "\n[bold yellow]Session expired during upload.[/bold yellow] "
            "Run [bold]ee-metadata login[/bold] then re-run this command. "
            "Already-uploaded files will be skipped automatically."
        )

    if failed or cancel_event.is_set():
        raise typer.Exit(code=1)
