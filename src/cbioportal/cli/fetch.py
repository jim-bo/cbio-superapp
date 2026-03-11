import typer
from cbioportal.core import fetcher
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Data fetching commands")
console = Console()

@app.command(name="list")
def list_studies(
    search: str = typer.Option(None, help="Filter studies by name or ID")
):
    """List all available studies on cbioportal.org."""
    try:
        studies = fetcher.list_remote_studies()
        
        table = Table(title="Available Remote Studies")
        table.add_column("Study ID", style="cyan")
        table.add_column("Name", style="green")
        
        count = 0
        for s in studies:
            sid = s.get('studyId', 'N/A')
            name = s.get('name', 'N/A')
            
            if search and search.lower() not in sid.lower() and search.lower() not in name.lower():
                continue
                
            table.add_row(sid, name[:70])
            count += 1
            
        console.print(table)
        console.print(f"\nFound {count} studies.")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")

@app.command()
def study(
    study_id: str,
    force: bool = typer.Option(False, "--force", "-f", help="Force redownload if exists")
):
    """Download a single study by ID."""
    try:
        result = fetcher.download_study(study_id, force=force)
        console.print(f"[green]{result}[/green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")

@app.command(name="all")
def download_all(
    limit: int = typer.Option(None, help="Limit the number of studies to download"),
    force: bool = typer.Option(False, "--force", "-f", help="Force redownload if exists")
):
    """Download multiple studies."""
    try:
        studies = fetcher.list_remote_studies()
        if limit:
            studies = studies[:limit]
            
        console.print(f"Starting download of {len(studies)} studies...")
        
        for s in studies:
            sid = s.get('studyId')
            try:
                result = fetcher.download_study(sid, force=force)
                console.print(f"[dim]{result}[/dim]")
            except Exception as e:
                console.print(f"[yellow]Skipped {sid}: {e}[/yellow]")
                
        console.print("\n[bold green]Batch download complete.[/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")

if __name__ == "__main__":
    app()
