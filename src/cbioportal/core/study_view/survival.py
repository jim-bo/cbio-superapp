"""Survival analysis: Kaplan-Meier curves and TMB/FGA scatter plot."""
from __future__ import annotations

from scipy import stats

from .filters import _build_filter_subquery, get_clinical_attributes, _get_mutation_sample_col

_EMPTY_SCATTER = {
    "bins": [], "pearson_corr": 0, "pearson_pval": 1,
    "spearman_corr": 0, "spearman_pval": 1,
    "count_min": 0, "count_max": 0,
    "x_bin_size": 0.025, "y_bin_size": 1.0,
}


def get_km_data(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> list[dict]:
    """Return [{time, survival}] KM curve points from OS_MONTHS + OS_STATUS.

    Biology:
        Kaplan-Meier survival analysis estimates the probability of surviving beyond
        each observed time point. The curve starts at 1.0 (100% survival at time 0)
        and steps down at each death event. Censored observations (patients who left
        the study without a death event) are accounted for by removing them from the
        at-risk pool at their censoring time but do not cause a step-down.

    Engineering:
        Queries OS_MONTHS (time) and OS_STATUS (event) from the patient table.
        Event = 1 when OS_STATUS contains 'deceased' or equals '1:DECEASED'.
        Delegates to compute_km_curve() for the pure math.
        Uses a sample → patient join so that filter_json (which operates on sample IDs)
        correctly restricts the patient cohort.

    Citation:
        Kaplan-Meier algorithm: Kaplan EL, Meier P. (1958) JASA 53:457-481.
        cBioPortal implementation: SurvivalTab.tsx + survivalCalc.ts in
        cbioportal-frontend (mirrors this algorithm exactly).
    """
    attrs = get_clinical_attributes(conn, study_id)
    time_col = None
    status_col = None
    source = "patient"

    for tc in ("OS_MONTHS", "os_months"):
        if tc in attrs:
            time_col = tc
            source = attrs[tc]
            break
    for sc in ("OS_STATUS", "os_status"):
        if sc in attrs:
            status_col = sc
            break

    if not time_col or not status_col:
        return []

    table = f'"{study_id}_{source}"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        sql = f"""
            SELECT
                TRY_CAST(p."{time_col}" AS DOUBLE) AS t,
                CASE
                    WHEN p."{status_col}" ILIKE '%deceased%' OR p."{status_col}" = '1:DECEASED' THEN 1
                    ELSE 0
                END AS event
            FROM "{study_id}_sample" s
            JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID
            WHERE s.SAMPLE_ID IN ({filter_sql})
            AND TRY_CAST(p."{time_col}" AS DOUBLE) IS NOT NULL
            ORDER BY t
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    pairs = [(r[0], r[1]) for r in rows if r[0] is not None]
    return compute_km_curve(pairs)


def compute_km_curve(pairs: list[tuple[float, int]]) -> list[dict]:
    """Compute Kaplan-Meier step function. pairs = [(time, event)] where event=1=death.

    Biology:
        At each unique death time t, the survival probability is updated as:
            S(t) = S(t_prev) × (n_at_risk - deaths) / n_at_risk
        Censored observations decrease n_at_risk without causing a survival step-down.
        The resulting curve is a non-increasing step function from 1.0 toward 0.0.

    Engineering:
        Pure Python implementation — no external survival library dependency.
        Input must be pre-sorted by time (or pass unsorted; this function sorts).
        Ties (multiple events at the same time) are processed together in one step.
        Returns only step-down points plus the t=0 anchor; interpolation is left
        to the frontend (ECharts 'step: end' mode).

    Returns:
        List of {time, survival} dicts. Always starts with {time: 0.0, survival: 1.0}.
        Returns [] for empty input.
    """
    if not pairs:
        return []
    pairs = sorted(pairs, key=lambda x: x[0])
    survival = 1.0
    n_at_risk = len(pairs)
    curve = [{"time": 0.0, "survival": 1.0}]
    i = 0
    while i < len(pairs):
        t = pairs[i][0]
        deaths = 0
        censored = 0
        j = i
        while j < len(pairs) and pairs[j][0] == t:
            if pairs[j][1] == 1:
                deaths += 1
            else:
                censored += 1
            j += 1
        if deaths > 0:
            survival *= (n_at_risk - deaths) / n_at_risk
            curve.append({"time": t, "survival": round(survival, 4)})
        n_at_risk -= (deaths + censored)
        i = j
    return curve


def get_tmb_fga_scatter(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> dict:
    """Return density-binned scatter data with Pearson/Spearman correlations.

    Biology:
        Tumor Mutational Burden (TMB, measured as mutation count per sample) and
        Fraction Genome Altered (FGA) are two orthogonal measures of genomic instability.
        High TMB often reflects defective mismatch repair (e.g. microsatellite instability);
        high FGA reflects chromosomal instability. Their correlation varies by cancer type.
        This scatter plot helps clinicians assess whether the cohort is dominated by one
        or both types of genomic instability.

    Engineering:
        Raw per-sample (FGA, mutation_count) pairs are density-binned into a 40×35 grid
        to avoid rendering thousands of individual points in the browser. Bin size is
        adaptive: x_bin_size = 1/40 (FGA is always 0-1), y_bin_size = max_mut / 35.
        Only samples with FGA > 0 and mutation_count > 0 are included.
        GERMLINE and Fusion mutations are excluded from mutation counting (mirrors
        cBioPortal's default TMB calculation).
        Pearson and Spearman correlations are computed on the raw un-binned data.

    Citation:
        FGA definition: |seg.mean| >= 0.2 threshold from cBioPortal FractionGenomeAltered.java.
        Correlation coefficients match cBioPortal's ScatterPlot.tsx exactl.
    """
    attrs = get_clinical_attributes(conn, study_id)
    fga_col = None
    for candidate in ("FRACTION_GENOME_ALTERED", "FGA"):
        if candidate in attrs:
            fga_col = candidate
            break
    if not fga_col:
        return _EMPTY_SCATTER

    sample_table = f'"{study_id}_sample"'
    mut_table = f'"{study_id}_mutations"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    mut_sample_col = _get_mutation_sample_col(conn, study_id)

    try:
        sql = f"""
            SELECT
                TRY_CAST(s."{fga_col}" AS DOUBLE) AS fga,
                COUNT(DISTINCT
                    CASE WHEN m.Chromosome IS NOT NULL
                    THEN CONCAT_WS('|', m.Chromosome,
                                   CAST(m.Start_Position AS VARCHAR),
                                   CAST(m.End_Position AS VARCHAR),
                                   m.Reference_Allele,
                                   m.Tumor_Seq_Allele1)
                    ELSE NULL END
                ) AS mutation_count
            FROM {sample_table} s
            LEFT JOIN {mut_table} m
                ON s.SAMPLE_ID = m.{mut_sample_col}
                AND COALESCE(m.Mutation_Status, '') <> 'GERMLINE'
                AND COALESCE(m.Variant_Classification, '') <> 'Fusion'
            WHERE s.SAMPLE_ID IN ({filter_sql})
            GROUP BY s.SAMPLE_ID, fga
            HAVING fga IS NOT NULL AND mutation_count > 0
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return _EMPTY_SCATTER

    if not rows:
        return _EMPTY_SCATTER

    fga_arr = [r[0] for r in rows]
    mut_arr = [r[1] for r in rows]

    if len(fga_arr) > 2:
        pearson_r, pearson_p = stats.pearsonr(fga_arr, mut_arr)
        spearman_r, spearman_p = stats.spearmanr(fga_arr, mut_arr)
    else:
        pearson_r = pearson_p = spearman_r = spearman_p = 0.0

    X_BINS, Y_BINS = 40, 35
    x_bin_size = 1.0 / X_BINS
    max_mut = max(mut_arr) if mut_arr else 1
    y_bin_size = max_mut / Y_BINS

    bin_counts: dict[tuple, int] = {}
    for fga_val, mut_val in zip(fga_arr, mut_arr):
        bx = round(min(int(fga_val / x_bin_size), X_BINS - 1) * x_bin_size, 6)
        by = round(int(mut_val / y_bin_size) * y_bin_size, 6)
        bin_counts[(bx, by)] = bin_counts.get((bx, by), 0) + 1

    counts = list(bin_counts.values())
    return {
        "bins": [{"bin_x": bx, "bin_y": by, "count": c}
                 for (bx, by), c in bin_counts.items()],
        "pearson_corr":  round(float(pearson_r), 4),
        "pearson_pval":  round(float(pearson_p), 4),
        "spearman_corr": round(float(spearman_r), 4),
        "spearman_pval": round(float(spearman_p), 4),
        "count_min": min(counts) if counts else 0,
        "count_max": max(counts) if counts else 0,
        "x_bin_size": x_bin_size,
        "y_bin_size": round(y_bin_size, 4),
    }
