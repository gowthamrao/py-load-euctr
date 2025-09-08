import pytest
import httpx
from pathlib import Path

from py_load_eurostat.fetcher import Fetcher
from py_load_eurostat.config import Settings

# Sample XML content for mocking the TOC response
MOCK_TOC_XML = """<?xml version="1.0" encoding="UTF-8"?>
<nt:tree xmlns:nt="urn:eu.europa.ec.eurostat.navtree">
  <nt:branch>
    <nt:children>
      <nt:leaf>
        <nt:code>nama_10_gdp</nt:code>
        <nt:title language="en">GDP and main components</nt:title>
      </nt:leaf>
      <nt:leaf>
        <nt:code>prc_hicp_manr</nt:code>
        <nt:title language="en">HICP - monthly data</nt:title>
      </nt:leaf>
    </nt:children>
  </nt:branch>
</nt:tree>
"""

MOCK_DATASET_CONTENT = "col1;col2\nval1;val2\n"


@pytest.fixture
def mock_settings(tmp_path: Path) -> Settings:
    """Fixture for mock settings using a temporary cache directory."""
    cache_path = tmp_path / "cache"
    cache_path.mkdir()
    return Settings(cache_dir=cache_path)


def test_list_available_datasets_success(mock_settings: Settings):
    """Test that list_available_datasets successfully parses a mocked TOC response."""

    def mock_handler(request: httpx.Request):
        return httpx.Response(200, text=MOCK_TOC_XML)

    transport = httpx.MockTransport(mock_handler)
    with httpx.Client(transport=transport) as client:
        fetcher = Fetcher(settings=mock_settings, client=client)
        datasets = fetcher.list_available_datasets()

        assert len(datasets) == 2
        assert datasets[0]["code"] == "nama_10_gdp"
        assert datasets[1]["title"] == "HICP - monthly data"


def test_download_dataset_success(mock_settings: Settings):
    """Test that a dataset is successfully downloaded to the cache."""
    dataset_code = "nama_10_gdp"

    def mock_handler(request: httpx.Request):
        if "catalogue/toc/xml" in str(request.url):
            return httpx.Response(200, text=MOCK_TOC_XML)
        # The URL for the dataset itself
        return httpx.Response(200, text=MOCK_DATASET_CONTENT)

    transport = httpx.MockTransport(mock_handler)
    with httpx.Client(transport=transport) as client:
        fetcher = Fetcher(settings=mock_settings, client=client)
        file_path = fetcher.download_dataset(dataset_code)

        assert file_path is not None
        assert file_path.exists()
        assert file_path.name == f"{dataset_code}.tsv.gz"

        # Check content (in a real scenario, you'd decompress, but here we mock uncompressed)
        with open(file_path, "r") as f:
            content = f.read()
        assert content == MOCK_DATASET_CONTENT


def test_get_toc_http_error(mock_settings: Settings, capsys):
    """Test that an HTTP error when fetching the TOC is handled gracefully."""

    def mock_handler(request: httpx.Request):
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(mock_handler)
    with httpx.Client(transport=transport) as client:
        fetcher = Fetcher(settings=mock_settings, client=client)
        result = fetcher.get_toc_content()

        assert result is None
        captured = capsys.readouterr()
        assert "500" in captured.out


def test_download_dataset_not_found_in_toc(mock_settings: Settings, capsys):
    """Test trying to download a dataset not present in the TOC."""

    def mock_handler(request: httpx.Request):
        return httpx.Response(200, text=MOCK_TOC_XML)

    transport = httpx.MockTransport(mock_handler)
    with httpx.Client(transport=transport) as client:
        fetcher = Fetcher(settings=mock_settings, client=client)
        result = fetcher.download_dataset("non_existent_code")

        assert result is None
        captured = capsys.readouterr()
        assert "not found in TOC" in captured.out


def test_download_dataset_http_error(mock_settings: Settings, capsys):
    """Test that an HTTP error during file download is handled gracefully."""
    dataset_code = "nama_10_gdp"

    def mock_handler(request: httpx.Request):
        if "catalogue/toc/xml" in str(request.url):
            return httpx.Response(200, text=MOCK_TOC_XML)
        return httpx.Response(404, text="Not Found")

    transport = httpx.MockTransport(mock_handler)
    with httpx.Client(transport=transport) as client:
        fetcher = Fetcher(settings=mock_settings, client=client)
        result = fetcher.download_dataset(dataset_code)

        assert result is None
        captured = capsys.readouterr()
        assert "404" in captured.out
        # Ensure partial file is cleaned up
        assert not (mock_settings.cache_dir / f"{dataset_code}.tsv.gz").exists()
