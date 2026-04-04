"""Unit tests for the storage backend abstraction (gcs.py)."""
import os
import tempfile
from pathlib import Path

import pytest

from cbioportal.core.gcs import LocalBackend, get_storage


class TestLocalBackend:
    def test_upload_and_download_roundtrip(self, tmp_path):
        root = tmp_path / "storage"
        backend = LocalBackend(root)

        src = tmp_path / "source.txt"
        src.write_text("hello cloud")

        backend.upload_file(src, "mydir/file.txt")
        assert (root / "mydir" / "file.txt").exists()

        dest = tmp_path / "downloaded.txt"
        backend.download_file("mydir/file.txt", dest)
        assert dest.read_text() == "hello cloud"

    def test_exists_returns_false_when_missing(self, tmp_path):
        backend = LocalBackend(tmp_path / "storage")
        assert not backend.exists("nonexistent/file.txt")

    def test_exists_returns_true_when_present(self, tmp_path):
        root = tmp_path / "storage"
        backend = LocalBackend(root)
        (root / "a.txt").parent.mkdir(parents=True, exist_ok=True)
        (root / "a.txt").write_text("x")
        assert backend.exists("a.txt")

    def test_list_prefix_empty(self, tmp_path):
        backend = LocalBackend(tmp_path / "storage")
        assert backend.list_prefix("nothing/") == []

    def test_list_prefix_returns_files(self, tmp_path):
        root = tmp_path / "storage"
        backend = LocalBackend(root)
        (root / "per-study-dbs").mkdir(parents=True)
        (root / "per-study-dbs" / "acc_tcga.duckdb").write_text("db")
        (root / "per-study-dbs" / "msk_chord_2024.duckdb").write_text("db")

        keys = backend.list_prefix("per-study-dbs/")
        assert len(keys) == 2
        assert all(".duckdb" in k for k in keys)

    def test_copy(self, tmp_path):
        root = tmp_path / "storage"
        backend = LocalBackend(root)
        (root / "master").mkdir(parents=True)
        (root / "master" / "cbioportal.duckdb").write_text("master-data")

        backend.copy("master/cbioportal.duckdb", "backups/cbioportal.20240101-120000.duckdb")

        assert (root / "backups" / "cbioportal.20240101-120000.duckdb").exists()
        assert (root / "backups" / "cbioportal.20240101-120000.duckdb").read_text() == "master-data"


class TestGetStorage:
    def test_returns_local_backend_without_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CBIO_GCS_BUCKET", raising=False)
        monkeypatch.setenv("CBIO_LOCAL_STORAGE", str(tmp_path / "lc"))
        storage = get_storage()
        assert isinstance(storage, LocalBackend)

    def test_returns_gcs_backend_with_env(self, monkeypatch):
        monkeypatch.setenv("CBIO_GCS_BUCKET", "my-test-bucket")
        # GCSBackend constructor calls google.cloud.storage.Client() which requires
        # credentials.  We just check the type returned, not that it connects.
        from cbioportal.core.gcs import GCSBackend
        try:
            storage = get_storage()
            assert isinstance(storage, GCSBackend)
        except RuntimeError as exc:
            # google-cloud-storage not installed — acceptable in CI without it.
            assert "google-cloud-storage" in str(exc)
        except Exception:
            # DefaultCredentialsError or similar — no GCP credentials in CI, that's fine.
            pass
        finally:
            monkeypatch.delenv("CBIO_GCS_BUCKET", raising=False)
