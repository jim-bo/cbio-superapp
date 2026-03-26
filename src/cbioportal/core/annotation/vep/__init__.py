"""vibe-vep integration for the annotation pipeline.

Public API:
    annotate_with_vep(conn, study_id, tmp_dir) -> dict | None

Returns a lookup dict keyed by (hugo, chr, start, ref, alt) → annotation fields,
or None if vibe-vep is not available or fails gracefully.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from .maf_io import detect_assembly, export_mutations_to_maf, parse_vep_output
from .runner import VepNotAvailableError, VepRuntimeError, is_vep_available, run_vep

logger = logging.getLogger(__name__)

__all__ = ["annotate_with_vep", "is_vep_available"]


def annotate_with_vep(
    conn,
    study_id: str,
    tmp_dir: Path | None = None,
) -> dict | None:
    """Run vibe-vep on study mutations and return a lookup dict.

    Returns None (not raises) if vibe-vep is not available.
    All vep_* columns will be NULL in that case.
    """
    if not is_vep_available():
        logger.info("vibe-vep not available — skipping transcript annotation")
        return None

    assembly = detect_assembly(conn, study_id)

    with tempfile.TemporaryDirectory(dir=tmp_dir) as td:
        tmp = Path(td)
        maf_in = tmp / f"{study_id}_input.maf"
        maf_out = tmp / f"{study_id}_vep.maf"

        try:
            n = export_mutations_to_maf(conn, study_id, maf_in)
            if n == 0:
                return {}
            run_vep(maf_in, maf_out, assembly=assembly)
            return parse_vep_output(maf_out)
        except VepNotAvailableError:
            logger.info("vibe-vep not available — skipping")
            return None
        except VepRuntimeError as e:
            logger.warning("vibe-vep failed, skipping VEP annotation: %s", e)
            return None
        except Exception as e:
            logger.warning("Unexpected error in VEP annotation, skipping: %s", e)
            return None
