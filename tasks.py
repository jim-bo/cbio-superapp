"""
Invoke tasks for Cloud Run deployment of cBioPortal Revamp.

Usage:
    inv build
    inv push
    inv deploy
    inv sync-db
    inv run-local
    inv logs
    inv migrate
    inv smoke-test
    inv load-test

Configuration — set via environment variables or override with -e:
    GCP_PROJECT      GCP project ID (required)
    GCP_REGION       Cloud Run region (default: us-central1)
    CBIO_GCS_BUCKET  GCS bucket containing cbioportal.duckdb (default: your-gcs-bucket)
    CBIO_DB_PATH     Local DuckDB path for sync-db / run-local (default: data/cbioportal.duckdb)

Example with overrides:
    inv deploy -e GCP_PROJECT=my-project -e GCP_REGION=us-east1
"""
import os
from invoke import task

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT = os.environ.get("GCP_PROJECT", "your-gcp-project-id")
REGION = os.environ.get("GCP_REGION", "us-central1")
SERVICE = "cbio-revamp"
REPO = "cbio"
IMAGE_NAME = "cbio-revamp"
GCS_BUCKET = os.environ.get("CBIO_GCS_BUCKET", "your-gcs-bucket")
DB_FILE = os.environ.get("CBIO_DB_PATH", "data/cbioportal.duckdb")

REGISTRY = f"{REGION}-docker.pkg.dev/{PROJECT}/{REPO}"
IMAGE = f"{REGISTRY}/{IMAGE_NAME}"


# ── Docker ────────────────────────────────────────────────────────────────────

@task(help={"tag": "Image tag (default: latest)"})
def build(c, tag="latest"):
    """Build the Docker image."""
    c.run(f"docker build -t {IMAGE}:{tag} -t {IMAGE}:latest .", echo=True)


@task(pre=[build], help={"tag": "Image tag to push (default: latest)"})
def push(c, tag="latest"):
    """Build and push image to Artifact Registry."""
    c.run(f"docker push {IMAGE}:{tag}", echo=True)
    if tag != "latest":
        c.run(f"docker push {IMAGE}:latest", echo=True)


# ── Cloud Run ─────────────────────────────────────────────────────────────────

@task(
    pre=[push],
    help={
        "tag": "Image tag to deploy (default: latest)",
        "sessions_db_url": "PostgreSQL URL — overrides CBIO_SESSIONS_DB_URL env var",
    },
)
def deploy(c, tag="latest", sessions_db_url=None):
    """Build, push, and deploy to Cloud Run.

    Memory: 8Gi  CPU: 2  Min: 0 (scale-to-zero)  Startup timeout: 300s
    CBIO_GCS_DB_URI triggers GCS download of DuckDB on cold start.
    """
    sessions_url = sessions_db_url or os.environ.get("CBIO_SESSIONS_DB_URL")
    if not sessions_url:
        print(
            "ERROR: Set CBIO_SESSIONS_DB_URL or pass --sessions-db-url.\n"
            "Example: postgresql+psycopg2://user:pass@/dbname?host=/cloudsql/PROJECT:REGION:INSTANCE"
        )
        raise SystemExit(1)

    gcs_db_uri = os.environ.get("CBIO_GCS_DB_URI", f"gs://{GCS_BUCKET}/cbioportal.duckdb")

    cmd = (
        f"gcloud run deploy {SERVICE}"
        f" --image {IMAGE}:{tag}"
        f" --region {REGION}"
        f" --platform managed"
        f" --memory 8Gi"
        f" --cpu 2"
        f" --min-instances 0"
        f" --max-instances 3"
        f" --port 8080"
        f" --timeout 300"
        f" --cpu-boost"
        f" --set-env-vars CBIO_GCS_DB_URI={gcs_db_uri}"
        f" --set-env-vars CBIO_SESSIONS_DB_URL={sessions_url}"
        f" --set-env-vars CBIO_SECURE_COOKIES=1"
        f" --set-env-vars CBIO_DB_PATH=/app/data/cbioportal.duckdb"
        f" --allow-unauthenticated"
        f" --project {PROJECT}"
    )
    c.run(cmd, echo=True)


# ── Data sync ─────────────────────────────────────────────────────────────────

@task(help={"db_file": "Local DuckDB path (default: data/cbioportal.duckdb)"})
def sync_db(c, db_file=DB_FILE):
    """Upload local DuckDB file to GCS.

    Uses parallel composite uploads for large files (threshold: 150 MB).
    Run this from your laptop whenever you want to refresh the deployed DB.
    """
    dest = f"gs://{GCS_BUCKET}/cbioportal.duckdb"
    c.run(
        f"gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp {db_file} {dest}",
        echo=True,
    )
    print(f"Uploaded {db_file} → {dest}")


# ── Local testing ─────────────────────────────────────────────────────────────

@task(
    help={
        "port": "Local port to bind (default: 8080)",
        "db_file": "Local DuckDB file to bind-mount (default: data/cbioportal.duckdb)",
    }
)
def run_local(c, port=8080, db_file=DB_FILE):
    """Run the Docker image locally using a bind-mounted DuckDB (no GCS download).

    For testing the GCS download path, run docker manually and pass
    -v ~/.config/gcloud:/root/.config/gcloud:ro with CBIO_GCS_DB_URI set.
    """
    abs_db = os.path.abspath(db_file)
    db_dir = os.path.dirname(abs_db)
    db_filename = os.path.basename(abs_db)

    cmd = (
        f"docker run --rm -it"
        f" -p {port}:8080"
        f" -v {db_dir}:/app/data:ro"
        f" -e CBIO_DB_PATH=/app/data/{db_filename}"
        f" -e CBIO_SESSIONS_DB_URL=sqlite:////tmp/sessions.db"
        f" -e CBIO_SECURE_COOKIES=0"
        f" -e PORT=8080"
        f" {IMAGE}:latest"
    )
    c.run(cmd, echo=True)


# ── Ops ───────────────────────────────────────────────────────────────────────

@task(help={"follow": "Stream logs (default: True)", "limit": "Max log lines (default: 50)"})
def logs(c, follow=True, limit=50):
    """Stream Cloud Run service logs."""
    follow_flag = "--follow" if follow else ""
    c.run(
        f"gcloud run services logs read {SERVICE}"
        f" --region {REGION}"
        f" --limit {limit}"
        f" {follow_flag}"
        f" --project {PROJECT}",
        echo=True,
    )


@task(help={"db_url": "PostgreSQL URL — overrides CBIO_SESSIONS_DB_URL env var"})
def migrate(c, db_url=None):
    """Run Alembic migrations against the production PostgreSQL DB.

    Reads CBIO_SESSIONS_DB_URL from the environment by default.
    alembic/env.py already handles this variable — no extra flags needed.

    Example:
        CBIO_SESSIONS_DB_URL=postgresql+psycopg2://user:pass@host/dbname inv migrate
        inv migrate --db-url postgresql+psycopg2://user:pass@host/dbname
    """
    env = dict(os.environ)
    if db_url:
        env["CBIO_SESSIONS_DB_URL"] = db_url

    if "CBIO_SESSIONS_DB_URL" not in env:
        print(
            "ERROR: Set CBIO_SESSIONS_DB_URL or pass --db-url.\n"
            "Example: postgresql+psycopg2://user:pass@host/dbname"
        )
        raise SystemExit(1)

    c.run("uv run alembic upgrade head", env=env, echo=True)


# ── Load testing ──────────────────────────────────────────────────────────────

@task(
    help={
        "host": "Target URL (default: http://localhost:8082)",
        "users": "Number of concurrent users (default: 2)",
        "duration": "Test duration, e.g. 30s, 2m (default: 30s)",
    }
)
def smoke_test(c, host="http://localhost:8082", users=2, duration="30s"):
    """Quick smoke test — 2 users, 30 s, verifies all endpoints respond.

    Requires the app to be running (inv run-local or a live URL).
    HTML report written to tests/load/smoke-report.html.
    """
    c.run(
        f"uv run locust -f tests/load/locustfile.py"
        f" --host {host}"
        f" --headless -u {users} -r 1 -t {duration}"
        f" --html tests/load/smoke-report.html"
        f" --only-summary",
        echo=True,
    )


@task(
    help={
        "host": "Target URL (default: http://localhost:8082)",
        "users": "Number of concurrent users (default: 20)",
        "spawn_rate": "Users spawned per second (default: 2)",
        "duration": "Test duration, e.g. 120s, 5m (default: 120s)",
    }
)
def load_test(c, host="http://localhost:8082", users=20, spawn_rate=2, duration="120s"):
    """Full load test — 20 concurrent users, 2 min, HTML report.

    Requires the app to be running (inv run-local or a live URL).
    HTML report written to tests/load/load-report.html.

    Example against Cloud Run:
        inv load-test --host https://cbio-revamp-xxx.run.app --users 50
    """
    c.run(
        f"uv run locust -f tests/load/locustfile.py"
        f" --host {host}"
        f" --headless -u {users} -r {spawn_rate} -t {duration}"
        f" --html tests/load/load-report.html",
        echo=True,
    )
