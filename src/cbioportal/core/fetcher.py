import os
import json
import requests
import tarfile
from pathlib import Path
from tqdm import tqdm
import typer

# Official asset gateway (bypasses direct S3 403s)
BASE_ASSET_URL = "https://datahub.assets.cbioportal.org"
# Official API to list all studies
STUDY_LIST_URL = "https://www.cbioportal.org/api/studies"
# GitHub LFS batch API for the datahub repo
LFS_BATCH_URL = "https://github.com/cBioPortal/datahub.git/info/lfs/objects/batch"

# Browser-like headers to perfectly simulate a Chrome user
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.cbioportal.org/datasets",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

LFS_HEADERS = {
    "Accept": "application/vnd.git-lfs+json",
    "Content-Type": "application/vnd.git-lfs+json",
}

_LFS_MAGIC = b"version https://git-lfs.github.com/spec/v1"


def get_download_dir():
    """Get the path to the download directory from the environment variable."""
    download_env = os.getenv("CBIO_DOWNLOADS")
    if not download_env:
        return Path(__file__).resolve().parent.parent.parent.parent / "downloads"

    download_path = Path(download_env)
    download_path.mkdir(parents=True, exist_ok=True)
    return download_path


def list_remote_studies():
    """Fetch the list of all available studies from the cBioPortal API."""
    response = requests.get(STUDY_LIST_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def _parse_lfs_pointer(path: Path) -> dict | None:
    """Return {oid, size} if path is an LFS pointer file, else None."""
    try:
        with open(path, "rb") as f:
            first = f.read(len(_LFS_MAGIC))
        if first != _LFS_MAGIC:
            return None
        # Parse the full pointer (tiny text file, safe to read all)
        text = path.read_text(errors="replace")
        oid = size = None
        for line in text.splitlines():
            if line.startswith("oid sha256:"):
                oid = line.split(":", 1)[1].strip()
            elif line.startswith("size "):
                size = int(line.split(" ", 1)[1].strip())
        if oid and size is not None:
            return {"oid": oid, "size": size}
    except Exception:
        pass
    return None


def _resolve_lfs_files(study_path: Path) -> None:
    """Find any LFS pointer files under study_path and replace them with real data."""
    pointers: list[tuple[Path, dict]] = []
    for f in study_path.rglob("*"):
        if not f.is_file():
            continue
        info = _parse_lfs_pointer(f)
        if info:
            pointers.append((f, info))

    if not pointers:
        return

    typer.echo(f"  Resolving {len(pointers)} large file(s) via LFS...")

    # Batch-request download URLs from GitHub LFS
    objects = [{"oid": info["oid"], "size": info["size"]} for _, info in pointers]
    resp = requests.post(
        LFS_BATCH_URL,
        headers=LFS_HEADERS,
        json={"operation": "download", "transfers": ["basic"], "objects": objects},
    )
    resp.raise_for_status()
    batch = resp.json()

    # Build oid → download URL map
    url_map: dict[str, str] = {}
    for obj in batch.get("objects", []):
        oid = obj.get("oid")
        dl = obj.get("actions", {}).get("download", {})
        href = dl.get("href")
        if oid and href:
            url_map[oid] = href

    # Download and replace each pointer
    for file_path, info in pointers:
        oid = info["oid"]
        size = info["size"]
        url = url_map.get(oid)
        if not url:
            raise Exception(f"LFS batch API did not return a download URL for {file_path.name} (oid={oid})")

        r = requests.get(url, stream=True, headers=HEADERS)
        r.raise_for_status()
        with open(file_path, "wb") as out, tqdm(
            desc=f"  {file_path.name}",
            total=size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in r.iter_content(chunk_size=65536):
                out.write(chunk)
                bar.update(len(chunk))


def download_study(study_id: str, force: bool = False):
    """Download and extract a study from the official asset gateway."""
    download_dir = get_download_dir()
    extract_path = download_dir / study_id
    tar_path = download_dir / f"{study_id}.tar.gz"

    if extract_path.exists() and not force:
        return f"Study {study_id} already exists locally. Use --force to redownload."

    url = f"{BASE_ASSET_URL}/{study_id}.tar.gz"

    try:
        download_dir.mkdir(parents=True, exist_ok=True)
        response = requests.get(url, stream=True, headers=HEADERS)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(tar_path, "wb") as f, tqdm(
            desc=f"Downloading {study_id}",
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                size = f.write(data)
                bar.update(size)

        typer.echo(f"Extracting {study_id}...")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=download_dir)

        tar_path.unlink()

        # Transparently resolve any LFS pointer files in the extracted study
        _resolve_lfs_files(download_dir / study_id)

        return f"Successfully downloaded and extracted {study_id} to {download_dir}"

    except Exception as e:
        if tar_path.exists():
            tar_path.unlink()
        raise Exception(f"Failed to download {study_id}: {e}")
