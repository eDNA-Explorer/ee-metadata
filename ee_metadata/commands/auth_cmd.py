"""Authentication commands for ee-metadata CLI."""

import typer
from rich.table import Table

from ee_metadata.auth import (
    DEFAULT_API_URL,
    AuthError,
    TokenExpiredError,
    decode_token_claims,
    exchange_code,
    generate_state,
    is_token_expiring_soon,
    open_browser,
    poll_device_token,
    refresh_access_token,
    request_device_code,
    start_callback_server,
    validate_token,
    wait_for_callback,
)
from ee_metadata.cli import app, console
from ee_metadata.token_storage import (
    TokenData,
    clear_token,
    get_token,
    storage_info,
    store_token,
)


def ensure_valid_token(token_data: TokenData) -> TokenData:
    """If access token is expiring soon and a refresh token exists,
    refresh transparently."""
    if not is_token_expiring_soon(token_data.token):
        return token_data
    if not token_data.refresh_token:
        return token_data  # Let validate_token() catch the expiry
    try:
        new_access, new_refresh = refresh_access_token(
            token_data.refresh_token, token_data.api_url
        )
        # Preserve the storage backend: if the token was stored in a
        # plaintext file (headless/no-keyring), pass insecure=True so
        # store_token() writes back to file instead of raising SystemExit.
        insecure = storage_info()["storage_method"] == "file"
        store_token(
            new_access,
            token_data.api_url,
            refresh_token=new_refresh,
            insecure=insecure,
        )
        console.print("[dim]Token refreshed automatically[/dim]")
        result = get_token()
        if result is None:
            return token_data  # Store succeeded but read failed; use original
        return result
    except (AuthError, TokenExpiredError):
        return token_data  # Let validate_token() catch the expiry


@app.command()
def login(
    api_url: str = typer.Option(
        "https://www.ednaexplorer.org",
        "--api-url",
        "-u",
        help="API URL (default: https://www.ednaexplorer.org)",
        envvar="EDNA_API_URL",
    ),
    no_browser: bool = typer.Option(
        False,
        "--no-browser",
        help="Skip automatic browser login; use manual token paste.",
    ),
    device: bool = typer.Option(
        False,
        "--device",
        help="Use device code flow (for headless/SSH environments).",
    ),
    insecure_storage: bool = typer.Option(
        False,
        "--insecure-storage",
        help="Store token in a plaintext file instead of the system keyring.",
    ),
):
    """Log in to eDNA Explorer.

    Opens a browser to authenticate, then automatically receives
    the token. Falls back to device code flow, then manual token paste.

    Use --device on headless servers (e.g. SSH into HPC clusters) to
    authenticate by entering a code on any device with a browser.
    """
    state = generate_state()
    console.print("\n[bold cyan]eDNA Explorer Login[/bold cyan]\n")

    if api_url.rstrip("/") != DEFAULT_API_URL.rstrip("/"):
        console.print(
            f"[bold yellow]Warning:[/bold yellow] Using non-default API URL: {api_url}\n"
            "Make sure you trust this server before authenticating.\n"
        )

    token = None
    refresh_token = None
    from_device = False

    # --- Attempt 1: Automatic browser flow ---
    if not no_browser and not device:
        try:
            server, port = start_callback_server()
        except OSError:
            server = None

        if server is not None:
            auth_url = f"{api_url}/cli/authorize?state={state}&port={port}"
            console.print("Opening browser for authentication...\n")
            console.print(f"  [link]{auth_url}[/link]\n")

            browser_opened = open_browser(auth_url)
            if not browser_opened:
                console.print("[yellow]Could not open browser automatically.[/yellow]")
                console.print("Please open the URL above manually.\n")

            console.print("[dim]Waiting for authorization...[/dim]\n")

            result = wait_for_callback(server)
            if result is not None:
                if result.state != state:
                    console.print(
                        "[bold red]Error:[/bold red] State mismatch in callback. "
                        "This may indicate a CSRF attack. Please try again."
                    )
                else:
                    # Exchange the short-lived code for a full token
                    console.print("[dim]Exchanging authorization code...[/dim]")
                    try:
                        exchange_result = exchange_code(result.code, api_url)
                        token = exchange_result.token
                        refresh_token = exchange_result.refresh_token
                    except AuthError as e:
                        console.print(f"[bold red]Error:[/bold red] {e}")

    # --- Attempt 2: Device code flow ---
    if token is None and not (no_browser and not device):
        if not device and not no_browser:
            console.print(
                "[yellow]Browser login did not complete. "
                "Trying device code flow...[/yellow]\n"
            )

        try:
            device_resp = request_device_code(api_url)

            console.print("[bold]Enter this code on any device with a browser:\n")
            console.print(f"  [bold cyan]{device_resp.user_code}[/bold cyan]\n")
            console.print(f"  URL: [link]{device_resp.verification_uri}[/link]\n")
            console.print(
                f"  Or open: [link]{device_resp.verification_uri_complete}[/link]\n"
            )

            with console.status("Waiting for authorization..."):
                poll_result = poll_device_token(
                    device_code=device_resp.device_code,
                    api_url=api_url,
                    interval=device_resp.interval,
                    expires_in=device_resp.expires_in,
                )
            token = poll_result.token
            refresh_token = poll_result.refresh_token
            from_device = True
        except AuthError as e:
            console.print(f"[bold red]Error:[/bold red] {e}\n")

    # --- Attempt 3: Manual paste fallback ---
    if token is None:
        if not no_browser:
            console.print("[yellow]Falling back to manual token paste.[/yellow]\n")

        auth_url = f"{api_url}/cli/authorize?state={state}"
        console.print("Open this URL to authenticate:\n")
        console.print(f"  [link]{auth_url}[/link]\n")

        token = typer.prompt("Paste your auth token here")
        if not token.strip():
            console.print("[bold red]Error:[/bold red] Token cannot be empty.")
            raise typer.Exit(code=1)
        token = token.strip()

    # --- Validate state embedded in JWT (skip for device flow tokens) ---
    if not from_device:
        try:
            claims = decode_token_claims(token)
            if claims.get("state") != state:
                console.print(
                    "[bold red]Error:[/bold red] Token was not generated for this session. "
                    "Please try again."
                )
                raise typer.Exit(code=1)
        except AuthError as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise typer.Exit(code=1) from None

    # --- Validate token server-side and save ---
    console.print("[dim]Validating token...[/dim]")

    try:
        user = validate_token(token, api_url)
        method = store_token(
            token, api_url, insecure=insecure_storage, refresh_token=refresh_token
        )
        display_name = user.name or user.email
        console.print(f"\n[bold green]✓ Logged in as {display_name}[/bold green]")
        console.print(f"[dim]Email: {user.email}[/dim]")
        if user.role == "ADMIN":
            console.print("[bold cyan]Role: Admin[/bold cyan]")
        if method == "keyring":
            console.print("[dim]Token stored securely in system keychain[/dim]")
        else:
            console.print(
                "[dim yellow]Token stored in plaintext file (insecure)[/dim yellow]"
            )
        console.print("[dim]Token expires in 2 hours[/dim]")
    except (AuthError, TokenExpiredError) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from None


@app.command()
def logout():
    """Log out of eDNA Explorer.

    Removes the locally stored authentication token.
    """
    if clear_token():
        console.print("[bold green]✓ Logged out[/bold green]")
    else:
        console.print("[yellow]Already logged out (no token found)[/yellow]")


@app.command("auth-status")
def auth_status():
    """Show authentication and token storage diagnostics."""
    info = storage_info()

    table = Table(title="Auth Status", show_header=False)
    table.add_column("Key", style="bold")
    table.add_column("Value")

    table.add_row("Keyring available", "yes" if info["keyring_available"] else "no")
    table.add_row("Keyring backend", info["backend"] or "n/a")
    table.add_row("Headless", "yes" if info["headless"] else "no")
    table.add_row("Storage method", info["storage_method"])
    table.add_row("Config dir", info["config_dir"])
    table.add_row("Token file", info["token_file"])

    token_data = get_token()
    if token_data is not None:
        token_data = ensure_valid_token(token_data)
        table.add_row("Authenticated", "[green]yes[/green]")
        table.add_row("API URL", token_data.api_url)
        try:
            user = validate_token(token_data.token, token_data.api_url)
            display_name = user.name or user.email
            table.add_row("User", display_name)
            if user.role == "ADMIN":
                table.add_row("Role", "[bold cyan]Admin[/bold cyan]")
            else:
                table.add_row("Role", "User")
        except (AuthError, TokenExpiredError) as e:
            table.add_row("Token valid", f"[red]no ({e})[/red]")
    else:
        table.add_row("Authenticated", "[yellow]no[/yellow]")

    console.print(table)
