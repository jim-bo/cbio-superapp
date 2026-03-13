# cli/

Typer CLI commands, registered in main entrypoint.

- `db.py` — Database commands: `add`, `remove`, `load-all`, `load-lfs`, `sync-*`

Before loading any study, call `loader.ensure_gene_reference(conn)` so Hugo symbol
normalization tables are available.
