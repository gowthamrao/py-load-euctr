import httpx
from typing import Optional, List, Dict
from pathlib import Path
import os

from .config import Settings
from .parser import parse_toc_xml, get_dataset_download_url


class Fetcher:
    """
    Handles all interactions with Eurostat APIs.
    """

    def __init__(self, settings: Settings, client: Optional[httpx.Client] = None):
        self.settings = settings
        self.toc_url = (
            "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml"
        )
        self.client = (
            client
            if client
            else httpx.Client(
                headers={"User-Agent": "py-load-eurostat/0.1.0"},
                follow_redirects=True,
                timeout=30.0,
            )
        )

        self.settings.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_toc_content(self) -> Optional[str]:
        """
        Retrieves the Table of Contents (TOC).
        """
        try:
            response = self.client.get(self.toc_url)
            response.raise_for_status()
            return response.text
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"An HTTP error occurred while fetching TOC: {e!r}")
            return None

    def list_available_datasets(self) -> List[Dict[str, str]]:
        """
        Gets the list of all available datasets from the TOC.
        """
        toc_content = self.get_toc_content()
        if not toc_content:
            return []
        return parse_toc_xml(toc_content)

    def download_dataset(self, dataset_code: str) -> Optional[Path]:
        """
        Downloads a specific dataset's TSV.GZ file to the local cache.
        """
        datasets = self.list_available_datasets()
        if not datasets:
            print("Could not retrieve dataset list from TOC.")
            return None

        download_url = get_dataset_download_url(datasets, dataset_code)
        if not download_url:
            print(f"Dataset with code '{dataset_code}' not found in TOC.")
            return None

        file_path = self.settings.cache_dir / f"{dataset_code}.tsv.gz"

        print(f"Downloading {dataset_code} from {download_url} to {file_path}...")
        try:
            with self.client.stream("GET", download_url) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
            print("Download complete.")
            return file_path
        except (httpx.RequestError, httpx.HTTPStatusError, httpx.ReadTimeout) as e:
            print(
                f"An error occurred while downloading the dataset '{dataset_code}': {e!r}"
            )
            if file_path.exists():
                os.remove(file_path)
            return None
