from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from dldbt import __version__
from dldbt.catalog.ducklake_pg import DuckLakePgAdapter
from dldbt.config import DEFAULT_CONFIG_FILENAME, Config, load_config
from dldbt.errors import DldbtError
from dldbt.git_ops import hooks as hook_ops
from dldbt.git_ops.branch import sanitize_branch_name
from dldbt.git_ops.match import branch_matches_any

app = typer.Typer(
    help="Map git branches to DuckLake schemas.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
branch_app = typer.Typer(help="Manage DuckLake branches.", no_args_is_help=True)
app.add_typer(branch_app, name="branch")

console = Console()
err_console = Console(stderr=True)


ConfigPathOption = Annotated[
    Path,
    typer.Option(
        "--config",
        "-c",
        help="Path to .dldbt.yml config file.",
        show_default=True,
    ),
]


def _load(config_path: Path) -> Config:
    try:
        return load_config(config_path)
    except DldbtError as e:
        err_console.print(f"[red]error:[/red] {e}")
        raise typer.Exit(code=2) from e


def _run(fn):
    try:
        fn()
    except DldbtError as e:
        err_console.print(f"[red]error:[/red] {e}")
        raise typer.Exit(code=1) from e


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(False, "--version", help="Print version and exit."),
) -> None:
    if version:
        console.print(f"dldbt {__version__}")
        raise typer.Exit()
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        raise typer.Exit()


@app.command()
def init(
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Initialize dldbt against the catalog referenced by the config."""
    config = _load(config_path)

    def go() -> None:
        with DuckLakePgAdapter(config) as adapter:
            adapter.init()
            main = adapter.get_branch(config.main_branch)
        console.print(
            f"[green]initialized[/green] dldbt at [bold]{config.storage.data_path}[/bold]"
        )
        if main is not None:
            console.print(
                f"  main branch: [cyan]{main.name}[/cyan] @ snapshot {main.base_snapshot_id}"
            )

    _run(go)


@branch_app.command("create")
def branch_create(
    name: Annotated[str, typer.Argument(help="git branch name to map")],
    from_: Annotated[
        str | None,
        typer.Option("--from", help="source branch (defaults to config main_branch)"),
    ] = None,
    git_commit: Annotated[
        str | None,
        typer.Option("--git-commit", help="commit sha to record against this branch"),
    ] = None,
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Shallow-copy `--from` into a new branch schema for `name`."""
    config = _load(config_path)
    schema_name = sanitize_branch_name(name)

    def go() -> None:
        with DuckLakePgAdapter(config) as adapter:
            src = from_ or config.main_branch
            base_snapshot = adapter.current_snapshot_id()
            copied = adapter.shallow_copy_schema(src, schema_name)
            record = adapter.register_branch(
                name=schema_name,
                git_branch=name,
                created_from=src,
                base_snapshot_id=base_snapshot,
                last_git_commit=git_commit,
            )
        console.print(
            f"[green]created[/green] branch [cyan]{record.name}[/cyan] "
            f"(git: [bold]{record.git_branch}[/bold]) "
            f"from [cyan]{record.created_from}[/cyan] — {copied} table(s) shallow-copied"
        )

    _run(go)


@branch_app.command("drop")
def branch_drop(
    name: Annotated[str, typer.Argument(help="git branch or schema name to drop")],
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Drop a branch schema. Shared parquet files are left in place."""
    config = _load(config_path)
    schema_name = sanitize_branch_name(name)

    def go() -> None:
        if schema_name in set(config.protected_branches):
            err_console.print(
                f"[red]refusing:[/red] {schema_name!r} is listed in "
                f"protected_branches and cannot be dropped via dldbt"
            )
            raise typer.Exit(code=1)
        with DuckLakePgAdapter(config) as adapter:
            adapter.drop_schema(schema_name)
            adapter.update_branch_status(schema_name, "abandoned")
        console.print(f"[green]dropped[/green] branch [cyan]{schema_name}[/cyan]")

    _run(go)


@branch_app.command("list")
def branch_list(
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """List every branch dldbt knows about."""
    config = _load(config_path)

    def go() -> None:
        with DuckLakePgAdapter(config) as adapter:
            branches = adapter.list_branches()
            sizes = {b.name: _schema_size(adapter, b.name) for b in branches}
        table = Table(show_header=True, header_style="bold")
        table.add_column("branch")
        table.add_column("git branch")
        table.add_column("from")
        table.add_column("base snap", justify="right")
        table.add_column("tables", justify="right")
        table.add_column("bytes", justify="right")
        table.add_column("commit")
        table.add_column("status")
        for b in branches:
            tcount, sbytes = sizes[b.name]
            table.add_row(
                b.name,
                b.git_branch,
                b.created_from,
                str(b.base_snapshot_id),
                str(tcount),
                _humanize_bytes(sbytes),
                (b.last_git_commit or "-")[:12],
                b.status,
            )
        console.print(table)

    _run(go)


@branch_app.command("show")
def branch_show(
    name: Annotated[str, typer.Argument(help="git branch or schema name")],
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Show branch metadata and its tables."""
    config = _load(config_path)
    schema_name = sanitize_branch_name(name)

    def go() -> None:
        with DuckLakePgAdapter(config) as adapter:
            record = adapter.get_branch(schema_name)
            if record is None:
                err_console.print(f"[red]not found:[/red] {schema_name}")
                raise typer.Exit(code=1)
            tables = adapter.list_tables(schema_name)
            current_snap = adapter.current_snapshot_id()
        console.print(f"[bold cyan]{record.name}[/bold cyan]  (git: {record.git_branch})")
        console.print(f"  status        : {record.status}")
        console.print(f"  created from  : {record.created_from}")
        console.print(f"  created at    : {record.created_at.isoformat()}")
        console.print(f"  base snapshot : {record.base_snapshot_id}")
        console.print(f"  current snap  : {current_snap}")
        console.print(f"  git commit    : {record.last_git_commit or '-'}")
        tbl = Table(show_header=True, header_style="bold")
        tbl.add_column("table")
        tbl.add_column("records", justify="right")
        tbl.add_column("bytes", justify="right")
        for t in tables:
            tbl.add_row(
                t.table_name,
                str(t.record_count) if t.record_count is not None else "-",
                _humanize_bytes(t.file_size_bytes) if t.file_size_bytes else "-",
            )
        console.print(tbl)

    _run(go)


def _schema_size(
    adapter: DuckLakePgAdapter, schema: str
) -> tuple[int, int]:
    tables = adapter.list_tables(schema)
    total_bytes = sum((t.file_size_bytes or 0) for t in tables)
    return len(tables), total_bytes


def _humanize_bytes(n: int | None) -> str:
    if n is None or n <= 0:
        return "0"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    val = float(n)
    while val >= 1024 and i < len(units) - 1:
        val /= 1024
        i += 1
    if i == 0:
        return f"{int(val)} {units[i]}"
    return f"{val:.1f} {units[i]}"


@app.command("install-hooks")
def install_hooks(
    project_root: Annotated[
        Path,
        typer.Option(
            "--project-root",
            help="Path to the git repo root (defaults to current directory).",
        ),
    ] = Path("."),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing non-dldbt hooks at the target paths.",
        ),
    ] = False,
) -> None:
    """Install dldbt's post-checkout and post-merge hooks into .git/hooks."""

    def go() -> None:
        results = hook_ops.install_all_hooks(
            project_root=project_root, force=force
        )
        for r in results:
            if r.action == "installed":
                console.print(f"[green]installed[/green] {r.hook} -> {r.path}")
            elif r.action == "updated":
                console.print(f"[yellow]updated[/yellow]   {r.hook} -> {r.path}")
            else:
                err_console.print(
                    f"[yellow]skipped[/yellow]   {r.hook} (foreign hook at "
                    f"{r.path}; re-run with --force to overwrite)"
                )

    _run(go)


@app.command("__post-checkout", hidden=True)
def _post_checkout(
    prev_head: str = typer.Argument(...),
    new_head: str = typer.Argument(...),
    flag: str = typer.Argument(...),
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Git post-checkout hook entry point. Non-fatal on every error path so
    we never break the user's `git checkout`."""
    try:
        if not hook_ops.is_branch_checkout(prev_head, new_head, flag):
            return
        if not config_path.exists():
            return
        config = load_config(config_path)
        if not config.auto_create.enabled:
            return
        git_branch = hook_ops.current_git_branch(config_path.parent.resolve())
        if git_branch is None:
            return  # detached HEAD or not a repo
        if branch_matches_any(git_branch, config.auto_create.skip_patterns):
            return
        schema_name = sanitize_branch_name(git_branch)
        with DuckLakePgAdapter(config) as adapter:
            if not adapter.is_initialized():
                err_console.print(
                    "[yellow]dldbt:[/yellow] catalog not initialized; "
                    "skipping auto-create (run `dldbt init`)"
                )
                return
            if adapter.get_branch(schema_name) is not None:
                return  # already registered
            src = config.main_branch
            base_snapshot = adapter.current_snapshot_id()
            adapter.shallow_copy_schema(src, schema_name)
            adapter.register_branch(
                name=schema_name,
                git_branch=git_branch,
                created_from=src,
                base_snapshot_id=base_snapshot,
                last_git_commit=new_head,
            )
        console.print(
            f"[green]dldbt:[/green] created schema "
            f"[cyan]{schema_name}[/cyan] from [cyan]{src}[/cyan] "
            f"for git branch [bold]{git_branch}[/bold]"
        )
    except DldbtError as e:
        err_console.print(f"[yellow]dldbt:[/yellow] {e}")


@app.command("__post-merge", hidden=True)
def _post_merge(
    _squashed: str = typer.Argument(..., metavar="SQUASHED"),
    config_path: ConfigPathOption = Path(DEFAULT_CONFIG_FILENAME),
) -> None:
    """Git post-merge hook entry point. Placeholder until phase 6 lands."""
    # Phase 6 will wire the promotion check here. For now we're a no-op so
    # merges into main don't print noise on every pull.
    return


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        err_console.print("\n[yellow]interrupted[/yellow]")
        sys.exit(130)


if __name__ == "__main__":
    main()
