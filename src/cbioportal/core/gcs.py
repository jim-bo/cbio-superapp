"""Storage abstraction for the cloud pipeline.

`get_storage()` returns a `GCSBackend` when `CBIO_GCS_BUCKET` is set, otherwise
a `LocalBackend` that treats remote paths as sub-paths of `CBIO_LOCAL_STORAGE`
(default: `./local-cloud/`).  All pipeline code calls `get_storage()` — nothing
else imports `google.cloud.storage` directly.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Protocol


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class StorageBackend(Protocol):
    def upload_file(self, local: Path, remote: str) -> None: ...
    def download_file(self, remote: str, local: Path) -> None: ...
    def exists(self, remote: str) -> bool: ...
    def list_prefix(self, prefix: str) -> list[str]: ...
    def copy(self, src: str, dst: str) -> None: ...


# ---------------------------------------------------------------------------
# GCS backend
# ---------------------------------------------------------------------------

class GCSBackend:
    """Uses google-cloud-storage. Activated when CBIO_GCS_BUCKET is set."""

    def __init__(self, bucket_name: str) -> None:
        try:
            from google.cloud import storage as gcs  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-storage is required for GCS operations. "
                "Install it with: pip install google-cloud-storage"
            ) from exc
        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._bucket_name = bucket_name

    def upload_file(self, local: Path, remote: str) -> None:
        blob = self._bucket.blob(remote)
        blob.upload_from_filename(str(local))

    def download_file(self, remote: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        blob = self._bucket.blob(remote)
        blob.download_to_filename(str(local))

    def exists(self, remote: str) -> bool:
        return self._bucket.blob(remote).exists()

    def list_prefix(self, prefix: str) -> list[str]:
        blobs = self._client.list_blobs(self._bucket_name, prefix=prefix)
        return [b.name for b in blobs]

    def copy(self, src: str, dst: str) -> None:
        src_blob = self._bucket.blob(src)
        self._bucket.copy_blob(src_blob, self._bucket, dst)


# ---------------------------------------------------------------------------
# Local backend (dev / CI without GCS)
# ---------------------------------------------------------------------------

class LocalBackend:
    """File-copy fallback for local dev. Remote paths are relative to CBIO_LOCAL_STORAGE."""

    def __init__(self, root: Path) -> None:
        self._root = root
        root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, remote: str) -> Path:
        return self._root / remote

    def upload_file(self, local: Path, remote: str) -> None:
        dest = self._resolve(remote)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local, dest)

    def download_file(self, remote: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._resolve(remote), local)

    def exists(self, remote: str) -> bool:
        return self._resolve(remote).exists()

    def list_prefix(self, prefix: str) -> list[str]:
        base = self._resolve(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return [
            str((p.relative_to(self._root)))
            for p in sorted(base.rglob("*"))
            if p.is_file()
        ]

    def copy(self, src: str, dst: str) -> None:
        dest = self._resolve(dst)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._resolve(src), dest)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_storage() -> StorageBackend:
    """Return the appropriate storage backend based on environment variables."""
    bucket = os.getenv("CBIO_GCS_BUCKET")
    if bucket:
        return GCSBackend(bucket)
    root = Path(os.getenv("CBIO_LOCAL_STORAGE", "local-cloud"))
    return LocalBackend(root)


def get_staging_path() -> Path:
    """Return the local path where study files are staged.

    In cloud: CBIO_DOWNLOADS points to /mnt/gcs/staging (GCS FUSE mount).
    Locally: CBIO_DOWNLOADS or ./local-cloud/staging/.
    """
    downloads_env = os.getenv("CBIO_DOWNLOADS")
    if downloads_env:
        return Path(downloads_env)
    root = Path(os.getenv("CBIO_LOCAL_STORAGE", "local-cloud"))
    staging = root / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    return staging
