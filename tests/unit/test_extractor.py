import asyncio
import re

import pytest
from httpx import AsyncClient, RequestError
from pytest_httpx import HTTPXMock

from py_load_euctr.extractor.euctr import EuctrExtractor

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_extractor_success(httpx_mock: HTTPXMock):
    """Tests the extractor's happy path, ensuring it fetches and yields data."""
    # Mock the API responses (R.7.3.2)
    httpx_mock.add_response(
        url=re.compile(r".*/search.*"),
        json={"results": [{"url": "http://test.com/1"}], "has_next_page": False},
    )
    httpx_mock.add_response(
        url="http://test.com/1", json={"trial_id": "1", "name": "Test A"}
    )

    async with AsyncClient() as client:
        extractor = EuctrExtractor(client=client)
        results = [res async for res in extractor.extract_trials()]

    assert len(results) == 1
    assert results[0]["trial_id"] == "1"


@pytest.mark.asyncio
async def test_extractor_retries_on_failure(httpx_mock: HTTPXMock, mocker):
    """Tests that the extractor's retry logic is triggered on transient errors."""
    # Mock a sequence of failures followed by a success
    httpx_mock.add_response(status_code=503, url="http://test.com/retry")
    httpx_mock.add_response(status_code=503, url="http://test.com/retry")
    httpx_mock.add_response(
        url="http://test.com/retry", json={"trial_id": "retry", "name": "Test Retry"}
    )

    # Spy on asyncio.sleep to verify backoff is happening
    spy_sleep = mocker.spy(asyncio, "sleep")

    async with AsyncClient() as client:
        # Lower the delay for faster tests and set max_retries
        extractor = EuctrExtractor(client=client, rate_limit_delay=0.01, max_retries=3)
        # We call _fetch_url directly to test the retry logic in isolation
        result = await extractor._fetch_url("http://test.com/retry")

    assert result is not None
    assert result["trial_id"] == "retry"
    # The request should be tried 3 times.
    # The first call to sleep is the rate limit, then two backoff sleeps.
    assert spy_sleep.call_count >= 3


@pytest.mark.asyncio
async def test_extractor_gives_up_after_max_retries(httpx_mock: HTTPXMock, caplog):
    """Tests that the extractor stops trying after max_retries are exhausted."""
    # Mock only failures for this URL
    httpx_mock.add_exception(RequestError("Connection failed"), url="http://test.com/fail")

    async with AsyncClient() as client:
        extractor = EuctrExtractor(client=client, max_retries=2)
        result = await extractor._fetch_url("http://test.com/fail")

    assert result is None
    # Check that a clear error was logged
    assert "All retries for http://test.com/fail failed." in caplog.text


@pytest.mark.asyncio
async def test_extractor_handles_empty_results(httpx_mock: HTTPXMock):
    """Tests that the extractor handles an empty search result gracefully."""
    httpx_mock.add_response(url=re.compile(r".*/search.*"), json={"results": []})

    async with AsyncClient() as client:
        extractor = EuctrExtractor(client=client)
        results = [res async for res in extractor.extract_trials()]

    assert len(results) == 0
