# syntax=docker/dockerfile:1
# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Copy dependency manifests first for layer-cache efficiency
COPY pyproject.toml uv.lock ./

# Install production deps into the project venv (no dev extras)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source and install the project itself
COPY src/ ./src/
RUN uv sync --frozen --no-dev

# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

# Non-root user — Cloud Run best practice
RUN addgroup --system cbio && adduser --system --ingroup cbio cbio

# Copy the uv-managed venv and project source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src ./src
COPY --from=builder /app/pyproject.toml ./pyproject.toml

# Copy alembic for migration support (inv migrate runs alembic in-process)
COPY alembic/ ./alembic/
COPY alembic.ini ./alembic.ini

# Venv bin first so the installed `cbio` entry point is found
ENV PATH="/app/.venv/bin:$PATH"

# Data directory — GCS download target or bind-mount for local testing
RUN mkdir -p /app/data && chown cbio:cbio /app/data

USER cbio

EXPOSE 8080

# Shell form required so Cloud Run's injected $PORT expands at runtime.
# --host 0.0.0.0 is required for Cloud Run to receive external traffic.
CMD ["sh", "-c", "python -m cbioportal.cli.main beta serve --host 0.0.0.0 --port ${PORT:-8080}"]
