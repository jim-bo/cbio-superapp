"""Hugo symbol normalization: 3-pass system for canonical gene names."""


def normalize_hugo_symbols(conn, study_id: str):
    """Normalize Hugo gene symbols in the mutations table to canonical names.

    Biology:
        Gene names are not stable — cBioPortal studies ship with symbols that were
        canonical at curation time but have since been renamed. For example, MLL2
        (a histone methyltransferase mutated in ~20% of B-cell lymphomas) was
        renamed KMT2D in 2013. Without normalization, the same gene appears under
        multiple names, fragmenting mutation counts and hiding true frequency.

    Engineering:
        Three-pass system, applied in priority order:
          1. Entrez Gene ID → canonical Hugo (most reliable; unaffected by symbol drift).
             Source: gene_reference table, loaded from cBioPortal datahub genes.json.
          2. Stale Hugo → current Hugo via rename table (~75 known renames).
             Source: gene_symbol_updates, loaded from cBioPortal's gene-update.md.
          3. NCBI alias → canonical Hugo (last resort; ~55k aliases).
             Source: gene_alias, loaded from seed-cbioportal_*.sql.gz.
        Pass 3 is needed for the KMT2 family where Entrez_Gene_Id=0 in some studies.

    Citation:
        Rename list: https://github.com/cBioPortal/datahub/blob/master/seedDB/gene-update.md
        (pinned 2024-11-01)
        Alias table: https://github.com/cBioPortal/datahub/blob/master/seedDB/
        Normalization logic mirrors:
        https://github.com/cBioPortal/cbioportal/blob/v5.4.7/core/src/main/java/
        org/mskcc/cbio/portal/scripts/NormalizeExpressionLevels.java (concept only —
        our implementation is DuckDB SQL rather than Java in-memory joins).
    """
    # Guard: only run if gene_reference exists and has rows
    try:
        count = conn.execute("SELECT COUNT(*) FROM gene_reference").fetchone()[0]
        if count == 0:
            return
    except Exception:
        return

    tables_res = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()
    existing_tables = {t[0] for t in tables_res}

    has_updates = "gene_symbol_updates" in existing_tables
    has_aliases = "gene_alias" in existing_tables

    mutations_table = f"{study_id}_mutations"
    cna_table = f"{study_id}_cna"

    # Pass 3 (CNA only): derive alias map from this study's mutations table *before*
    # mutations are normalized, so stale symbols (e.g. MLL2) are still present.
    if cna_table in existing_tables and mutations_table in existing_tables:
        conn.execute(f"""
            UPDATE "{cna_table}"
            SET hugo_symbol = alias_map.canonical
            FROM (
                SELECT DISTINCT
                    "{mutations_table}".Hugo_Symbol AS old_symbol,
                    gr.hugo_gene_symbol            AS canonical
                FROM "{mutations_table}"
                JOIN gene_reference gr
                  ON TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id
                WHERE TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) > 0
                  AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
            ) alias_map
            WHERE "{cna_table}".hugo_symbol = alias_map.old_symbol
        """)

    if mutations_table in existing_tables:
        # Pass 1: normalize by Entrez Gene ID.
        # Qualify columns with table name to avoid case-insensitive ambiguity with gene_reference.entrez_gene_id
        conn.execute(f"""
            UPDATE "{mutations_table}"
            SET Hugo_Symbol = gr.hugo_gene_symbol
            FROM gene_reference gr
            WHERE TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id
              AND TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) > 0
              AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
        """)
        # Pass 2: normalize by symbol map (covers renamed genes in gene-update.md)
        if has_updates:
            conn.execute(f"""
                UPDATE "{mutations_table}"
                SET Hugo_Symbol = su.new_symbol
                FROM gene_symbol_updates su
                WHERE "{mutations_table}".Hugo_Symbol = su.old_symbol
            """)
        # Pass 3: normalize by gene_alias table (covers historical aliases like MLL2→KMT2D)
        if has_aliases:
            conn.execute(f"""
                UPDATE "{mutations_table}"
                SET Hugo_Symbol = gr.hugo_gene_symbol
                FROM gene_alias ga
                JOIN gene_reference gr ON ga.entrez_gene_id = gr.entrez_gene_id
                WHERE "{mutations_table}".Hugo_Symbol = ga.alias_symbol
                  AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
            """)

    if cna_table in existing_tables:
        # CNA symbol map normalization (covers cases not bridged via mutations)
        if has_updates:
            conn.execute(f"""
                UPDATE "{cna_table}"
                SET hugo_symbol = su.new_symbol
                FROM gene_symbol_updates su
                WHERE hugo_symbol = su.old_symbol
            """)
        if has_aliases:
            conn.execute(f"""
                UPDATE "{cna_table}"
                SET hugo_symbol = gr.hugo_gene_symbol
                FROM gene_alias ga
                JOIN gene_reference gr ON ga.entrez_gene_id = gr.entrez_gene_id
                WHERE hugo_symbol = ga.alias_symbol
                  AND hugo_symbol IS DISTINCT FROM gr.hugo_gene_symbol
            """)
