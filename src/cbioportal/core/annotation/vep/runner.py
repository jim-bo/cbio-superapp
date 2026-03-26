"""vibe-vep runner — subprocess wrapper with graceful degradation.

vibe-vep installation (optional):
    go install github.com/inodb/vibe-vep/cmd/vibe-vep@latest
    vibe-vep download                          # GRCh38 core GENCODE transcripts (~95MB)
    vibe-vep download --assembly GRCh37        # GRCh37 transcripts (~95MB)
    # Optional AlphaMissense:
    vibe-vep config set annotations.alphamissense true && vibe-vep download
    # Optional hotspots:
    vibe-vep config set annotations.hotspots true && vibe-vep download
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


class VepNotAvailableError(RuntimeError):
    """Raised when vibe-vep binary is not on PATH."""


class VepRuntimeError(RuntimeError):
    """Raised when vibe-vep subprocess exits non-zero."""


def is_vep_available() -> bool:
    """Return True if the vibe-vep binary is on PATH."""
    return shutil.which("vibe-vep") is not None


def run_vep(
    maf_path: Path,
    out_path: Path,
    assembly: str = "GRCh38",
    timeout: int = 600,
) -> None:
    """Run vibe-vep on a MAF file and write annotated output to out_path.

    Args:
        maf_path:  Input MAF file (exported from DuckDB).
        out_path:  Destination for annotated MAF output.
        assembly:  Genome assembly: 'GRCh37' or 'GRCh38' (default GRCh38).
        timeout:   Subprocess timeout in seconds (default 10 min).

    Raises:
        VepNotAvailableError: If vibe-vep is not installed.
        VepRuntimeError:      If vibe-vep exits with a non-zero status.
    """
    if not is_vep_available():
        raise VepNotAvailableError(
            "vibe-vep not found on PATH. "
            "Install with: go install github.com/inodb/vibe-vep/cmd/vibe-vep@latest"
        )

    cmd = [
        "vibe-vep", "annotate", "maf",
        "--assembly", assembly,
        "-o", str(out_path),
        str(maf_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    if result.returncode != 0:
        raise VepRuntimeError(
            f"vibe-vep exited {result.returncode}:\n{result.stderr}"
        )
