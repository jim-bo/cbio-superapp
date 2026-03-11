# cBioPortal Revamp Implementation

A modern, fast, and lightweight version of cBioPortal using FastAPI, HTMX, Alpine.js, and DuckDB.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) for package management.
- A local clone of the [cBioPortal Datahub](https://github.com/cBioPortal/datahub).

## Setup

1. Set the `CBIO_DATAHUB` environment variable:
   ```bash
   export CBIO_DATAHUB=/path/to/your/datahub
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

## CLI Usage

Run the `cbioportal` command:
```bash
uv run cbioportal --help
```

### Load all studies:
```bash
uv run cbioportal db load-all
```

### Serve the web app:
```bash
uv run cbioportal serve
```
