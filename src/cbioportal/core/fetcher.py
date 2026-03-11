import os
import requests
import tarfile
from pathlib import Path
from tqdm import tqdm
import typer

# Official asset gateway (bypasses direct S3 403s)
BASE_ASSET_URL = "https://datahub.assets.cbioportal.org"
# Official API to list all studies
STUDY_LIST_URL = "https://www.cbioportal.org/api/studies"

# Browser-like headers to perfectly simulate a Chrome user
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.cbioportal.org/datasets",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def get_download_dir():
    """Get the path to the download directory from the environment variable."""
    download_env = os.getenv("CBIO_DOWNLOADS")
    if not download_env:
        # Default to a 'downloads' folder in the project root
        return Path(__file__).resolve().parent.parent.parent.parent / "downloads"
    
    download_path = Path(download_env)
    download_path.mkdir(parents=True, exist_ok=True)
    return download_path

def list_remote_studies():
    """Fetch the list of all available studies from the cBioPortal API."""
    response = requests.get(STUDY_LIST_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json()

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
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(tar_path, 'wb') as f, tqdm(
            desc=f"Downloading {study_id}",
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                size = f.write(data)
                bar.update(size)

        # Extract
        typer.echo(f"Extracting {study_id}...")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=download_dir)
            
        # Clean up the tarball
        tar_path.unlink()
        return f"Successfully downloaded and extracted {study_id} to {download_dir}"

    except Exception as e:
        if tar_path.exists():
            tar_path.unlink()
        raise Exception(f"Failed to download {study_id}: {e}")
