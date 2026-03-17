# cBioPortal-cli

An unofficial technology demonstration of what you can build with agentic coding tools.
This should not be used for serious work, but you are more than welcome to explore
this offshoot. Feedback is totally welcome.

## Overview

A cbioportal CLI with a minimal Python stack that includes a rich, full-screen Terminal UI (TUI) for interacting with APIs, querying studies, and exporting customized genomic datasets.

![Interface Demonstration](docs/img/tui_demo.gif)

## Installation

### Use it (pip / uv)

No clone required. Install directly from GitHub:

```bash
# with pip
pip install git+https://github.com/jim-bo/cbio-cli.git

# with uv
uv pip install git+https://github.com/jim-bo/cbio-cli.git
```

This installs the `cbio` CLI entry point. Then run:

```bash
cbio
```

### Development Setup

Clone the repo and install with dev dependencies:

```bash
git clone https://github.com/jim-bo/cbio-cli.git
cd cbio-cli
uv sync
uv run cbio
```

**Optional:** [Docker](https://www.docker.com/) is required only for validating MAF exports (`--run-docker`).

## TUI Reference

The primary interface is the interactive Terminal UI.

### `cbio` (Interactive TUI)

Launch the full-screen interactive REPL:

```bash
cbio
```

**Features & Commands within the TUI:**
- `/search [query]`: Search the public cBioPortal for studies.
- `pull`: Start the interactive data-pulling wizard to fetch, annotate (via MoAlmanac), and export MAF files for a selected study.
- `/config`: View the current backend configuration.
- `exit` (or press `Ctrl+D` twice): Safely exit the application.

*Note: The TUI caches API responses in a dedicated DuckDB file at `~/.cbio/cache/cache.duckdb` for high-speed offline analysis.*

## Testing (Core TUI)

### Core Tests 

These tests cover the TUI, API clients, and core DuckDB logic.

```bash
uv run pytest tests/unit/ tests/integration/ -v
```

*Note: Use `--run-live-api` and `--run-docker` for full API and export validation.*

---

## Experimental Features

Documentation for the experimental local web app and datahub ingestion can be found in [BETA.md](BETA.md).
