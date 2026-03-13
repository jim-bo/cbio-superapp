"""Unit tests for get_mutated_genes() using in-memory DuckDB.

cBioPortal reference: ClickhouseAlterationMapper.xml getMutatedGenes query.
The only hard-coded exclusion is mutation_status != 'UNCALLED'.
All variant classifications (including Silent, Intron, etc.) are counted by default;
optional mutation-type filtering is only applied when the caller provides
an explicit alterationFilter in the request.
"""
import pytest

from cbioportal.core.study_view_repository import get_mutated_genes
from tests.unit.conftest import STUDY


def _insert(db, sample_id, gene, vc, status="Somatic"):
    db.execute(
        f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (sample_id, f"P_{sample_id}")
    )
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        (sample_id, gene, None, vc, status),
    )


def _result_by_gene(rows, gene):
    return next((r for r in rows if r["gene"] == gene), None)


def test_missense_counted(db):
    _insert(db, "S1", "KRAS", "Missense_Mutation")
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_samples"] == 1


def test_silent_mutation_counted(db):
    """Silent mutations ARE counted — cBioPortal includes all VCs by default.

    Ref: ClickhouseAlterationMapper.xml — no variant_classification filter in getMutatedGenes.
    """
    _insert(db, "S1", "KRAS", "Silent")
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_samples"] == 1


def test_uncalled_excluded(db):
    """mutation_status='UNCALLED' IS excluded — the only hard-coded filter.

    Ref: ClickhouseAlterationMapper.xml line 21: mutation_status != 'UNCALLED'
    Comment: 'UnCalled is only used in Patient View to see supporting reads'
    """
    _insert(db, "S1", "KRAS", "Missense_Mutation", status="UNCALLED")
    result = get_mutated_genes(db, STUDY)
    assert _result_by_gene(result, "KRAS") is None


def test_silent_and_missense_same_sample_counted_once(db):
    """One sample with both Silent and Missense: counted once (deduped by SAMPLE_ID)."""
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S1", "P_S1"))
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Silent", "Somatic"),
    )
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Missense_Mutation", "Somatic"),
    )
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_samples"] == 1


def test_two_samples_both_counted(db):
    """Sample A: Missense, Sample B: Silent — both counted (Silent is not excluded)."""
    _insert(db, "A", "KRAS", "Missense_Mutation")
    _insert(db, "B", "KRAS", "Silent")
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_samples"] == 2


def test_null_variant_classification_counted(db):
    """NULL Variant_Classification is NOT excluded (no VC filter exists)."""
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S1", "P_S1"))
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, None, "Somatic"),
    )
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_samples"] == 1


def test_n_mut_counts_all_non_uncalled(db):
    """n_mut = total rows excluding UNCALLED; n_samples = distinct samples."""
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S1", "P_S1"))
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S2", "P_S2"))
    # 2 Missense in different samples
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Missense_Mutation", "Somatic"),
    )
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S2", "KRAS", None, "Missense_Mutation", "Somatic"),
    )
    # 1 Silent in S1 — counted in n_mut, not duplicating n_samples
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Silent", "Somatic"),
    )
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_mut"] == 3    # all 3 rows included
    assert kras["n_samples"] == 2


def test_freq_calculation(db):
    """10 samples in study, 4 have KRAS mutation → freq=40.0."""
    for i in range(10):
        db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (f"S{i}", f"P{i}"))
    for i in range(4):
        db.execute(
            f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
            (f"S{i}", "KRAS", None, "Missense_Mutation", "Somatic"),
        )
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["freq"] == 40.0


def test_uncalled_not_counted_in_n_mut(db):
    """UNCALLED rows must not appear in n_mut or n_samples."""
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S1", "P_S1"))
    # One real mutation, one UNCALLED
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Missense_Mutation", "Somatic"),
    )
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", None, "Missense_Mutation", "UNCALLED"),
    )
    result = get_mutated_genes(db, STUDY)
    kras = _result_by_gene(result, "KRAS")
    assert kras is not None
    assert kras["n_mut"] == 1
    assert kras["n_samples"] == 1
