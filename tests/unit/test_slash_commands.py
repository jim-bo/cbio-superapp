"""Unit tests for slash command resolver and slash commands.

Uses in-memory DuckDB and a fake app object — no real DB, no LLM.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from unittest.mock import patch

import duckdb
import pytest

from cbioportal.cli.slash_commands._resolve import resolve_study_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def conn():
    """In-memory DuckDB seeded with a minimal studies + study_data_types schema."""
    db = duckdb.connect(":memory:")
    db.execute(
        """
        CREATE TABLE studies (
            study_id     VARCHAR,
            type_of_cancer VARCHAR,
            name         VARCHAR,
            description  VARCHAR,
            short_name   VARCHAR,
            public_study BOOLEAN,
            pmid         VARCHAR,
            citation     VARCHAR,
            groups       VARCHAR,
            category     VARCHAR
        )
        """
    )
    db.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("msk_chord_2024", "mixed", "MSK CHORD 2024", "desc", "chord", True, None, None, None, "Lung"),
    )
    db.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("brca_tcga", "breast", "Breast Cancer TCGA", "desc2", "brca", True, None, None, None, "Breast"),
    )
    db.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("lung_cancer_2021", "lung", "Lung Cancer Atlas 2021", "desc3", "lung", True, None, None, None, "Lung"),
    )
    db.execute(
        "CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR)"
    )
    db.execute(
        "INSERT INTO study_data_types VALUES (?, ?), (?, ?), (?, ?)",
        ("msk_chord_2024", "mutation", "msk_chord_2024", "cna", "brca_tcga", "mutation"),
    )
    yield db
    db.close()


class FakeApp:
    """Minimal fake app object with a history list."""

    def __init__(self):
        self.history: list[str] = []

    def add_to_history(self, text: str) -> None:
        self.history.append(text)

    def combined_history(self) -> str:
        return "\n".join(self.history)


def make_fake_open_conn(db):
    """Return a context-manager factory that yields the given connection."""

    @contextmanager
    def fake_open_conn(read_only: bool = True):
        yield db

    return fake_open_conn


# ---------------------------------------------------------------------------
# resolve_study_id tests
# ---------------------------------------------------------------------------


def test_resolve_study_id_exact_id(conn):
    resolved, candidates = resolve_study_id(conn, "msk_chord_2024")
    assert resolved == "msk_chord_2024"
    assert len(candidates) == 1
    assert candidates[0]["study_id"] == "msk_chord_2024"


def test_resolve_study_id_case_insensitive(conn):
    resolved, candidates = resolve_study_id(conn, "MSK_CHORD_2024")
    assert resolved == "msk_chord_2024"


def test_resolve_study_id_exact_name(conn):
    resolved, candidates = resolve_study_id(conn, "Breast Cancer TCGA")
    assert resolved == "brca_tcga"
    assert len(candidates) == 1


def test_resolve_study_id_substring_unique(conn):
    """'chord' appears in only msk_chord_2024 — single match should resolve."""
    resolved, candidates = resolve_study_id(conn, "chord")
    assert resolved == "msk_chord_2024"
    assert len(candidates) == 1


def test_resolve_study_id_substring_ambiguous(conn):
    """'cancer' matches brca_tcga name and lung_cancer_2021 — should be ambiguous."""
    resolved, candidates = resolve_study_id(conn, "cancer")
    assert resolved is None
    assert len(candidates) >= 2


def test_resolve_study_id_missing(conn):
    resolved, candidates = resolve_study_id(conn, "zzz_nothing")
    assert resolved is None
    assert candidates == []


def test_resolve_study_id_empty(conn):
    resolved, candidates = resolve_study_id(conn, "")
    assert resolved is None
    assert candidates == []

    resolved2, candidates2 = resolve_study_id(conn, "   ")
    assert resolved2 is None
    assert candidates2 == []


def test_resolve_study_id_ranking_shorter_id_wins(conn):
    """Add two studies that both substring-match 'short'; the shorter study_id ranks first."""
    conn.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("short_a", "mixed", "Short Alpha Study", "d", "s", True, None, None, None, "Lung"),
    )
    conn.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("short_beta_extended", "mixed", "Short Beta Extended Study", "d", "s", True, None, None, None, "Lung"),
    )
    resolved, candidates = resolve_study_id(conn, "short")
    assert resolved is None  # multiple matches → ambiguous
    # shorter study_id should come first among candidates
    assert candidates[0]["study_id"] == "short_a"


# ---------------------------------------------------------------------------
# StudyInfoCommand tests
# ---------------------------------------------------------------------------


def test_study_info_unknown_study(conn):
    from cbioportal.cli.slash_commands.study_info_cmd import StudyInfoCommand

    fake_app = FakeApp()
    fake_conn = make_fake_open_conn(conn)

    with patch("cbioportal.cli.slash_commands.study_info_cmd.open_conn", fake_conn):
        _run(StudyInfoCommand().execute(fake_app, ["zzz_nothing"]))

    combined = fake_app.combined_history()
    assert "No study matched" in combined or "No study" in combined.lower() or "matched" in combined.lower()


def test_study_info_ambiguous(conn):
    from cbioportal.cli.slash_commands.study_info_cmd import StudyInfoCommand

    fake_app = FakeApp()
    fake_conn = make_fake_open_conn(conn)

    # "cancer" is ambiguous — matches brca_tcga and lung_cancer_2021
    with patch("cbioportal.cli.slash_commands.study_info_cmd.open_conn", fake_conn):
        # Also patch describe_study so it doesn't try to open a real DB
        with patch("cbioportal.cli.slash_commands.study_info_cmd.describe_study"):
            _run(StudyInfoCommand().execute(fake_app, ["cancer"]))

    combined = fake_app.combined_history()
    # Should contain a disambiguation table with study_id column headers or rows
    assert "study_id" in combined or "Multiple" in combined or "brca_tcga" in combined or "lung_cancer" in combined


# ---------------------------------------------------------------------------
# CancerTypesCommand tests
# ---------------------------------------------------------------------------


def test_cancer_types_renders_both_tables(conn):
    from cbioportal.cli.slash_commands.cancer_types_cmd import CancerTypesCommand

    # Seed one PanCancer Studies entry so special collections is non-empty
    conn.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("pancancer_atlas", "mixed", "Pan-Cancer Atlas", "d", "pc", True, None, None, None, "PanCancer Studies"),
    )

    fake_app = FakeApp()
    fake_conn = make_fake_open_conn(conn)

    with patch("cbioportal.cli.slash_commands.cancer_types_cmd.open_conn", fake_conn):
        _run(CancerTypesCommand().execute(fake_app, []))

    combined = fake_app.combined_history()
    assert "Organ systems" in combined
    assert "Special collections" in combined


# ---------------------------------------------------------------------------
# DataTypesCommand tests
# ---------------------------------------------------------------------------


def test_data_types_renders_list(conn):
    from cbioportal.cli.slash_commands.data_types_cmd import DataTypesCommand

    fake_app = FakeApp()
    fake_conn = make_fake_open_conn(conn)

    with patch("cbioportal.cli.slash_commands.data_types_cmd.open_conn", fake_conn):
        _run(DataTypesCommand().execute(fake_app, ["msk_chord_2024"]))

    combined = fake_app.combined_history()
    # msk_chord_2024 has mutation + cna seeded in study_data_types
    assert "mutation" in combined
    assert "cna" in combined


# ---------------------------------------------------------------------------
# GenesCommand tests
# ---------------------------------------------------------------------------


def test_genes_missing_args(conn):
    from cbioportal.cli.slash_commands.genes_cmd import GenesCommand

    fake_app = FakeApp()
    fake_conn = make_fake_open_conn(conn)

    with patch("cbioportal.cli.slash_commands.genes_cmd.open_conn", fake_conn):
        _run(GenesCommand().execute(fake_app, []))

    combined = fake_app.combined_history()
    assert "Usage:" in combined or "usage" in combined.lower()
