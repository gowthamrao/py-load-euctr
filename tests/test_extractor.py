import pytest
import httpx
from pytest_httpx import HTTPXMock

from py_load_euctr.extractor import CtisExtractor
from py_load_euctr.config import Settings

# Mocks for the CTIS API
MOCK_SEARCH_RESPONSE_PAGE_1 = {
    "pagination": {"page": 1, "size": 2, "totalPages": 2, "nextPage": True},
    "data": [
        {"ctNumber": "2022-000001-01", "ctTitle": "Trial 1"},
        {"ctNumber": "2022-000002-02", "ctTitle": "Trial 2"},
    ],
}

MOCK_SEARCH_RESPONSE_PAGE_2 = {
    "pagination": {"page": 2, "size": 2, "totalPages": 2, "nextPage": False},
    "data": [
        {"ctNumber": "2022-000003-03", "ctTitle": "Trial 3"},
    ],
}

MOCK_TRIAL_DETAILS_1 = {"ctNumber": "2022-000001-01", "details": "Details for Trial 1"}
MOCK_TRIAL_DETAILS_2 = {"ctNumber": "2022-000002-02", "details": "Details for Trial 2"}
MOCK_TRIAL_DETAILS_3 = {"ctNumber": "2022-000003-03", "details": "Details for Trial 3"}


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for mock settings."""
    return Settings()


@pytest.mark.asyncio
async def test_ctis_extractor_happy_path(mock_settings: Settings, httpx_mock: HTTPXMock):
    """
    Tests that the CtisExtractor can successfully fetch trials, handle pagination,
    and yield the full details for each trial.
    """
    # Mock the search API responses
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json=MOCK_SEARCH_RESPONSE_PAGE_1,
        match_json={"pagination": {"page": 1, "size": 20}, "sort": {"property": "decisionDate", "direction": "DESC"}},
    )
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json=MOCK_SEARCH_RESPONSE_PAGE_2,
        match_json={"pagination": {"page": 2, "size": 20}, "sort": {"property": "decisionDate", "direction": "DESC"}},
    )

    # Mock the retrieve API responses
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000001-01"),
        json=MOCK_TRIAL_DETAILS_1,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000002-02"),
        json=MOCK_TRIAL_DETAILS_2,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000003-03"),
        json=MOCK_TRIAL_DETAILS_3,
    )

    extractor = CtisExtractor(settings=mock_settings)

    # Run the extraction and collect results
    results = [trial async for trial in extractor.extract_trials()]

    # Sort results by ctNumber to ensure a deterministic order for assertions
    results.sort(key=lambda x: x["ctNumber"])

    # Assertions
    assert len(results) == 3
    assert results[0] == MOCK_TRIAL_DETAILS_1
    assert results[1] == MOCK_TRIAL_DETAILS_2
    assert results[2] == MOCK_TRIAL_DETAILS_3


@pytest.mark.asyncio
async def test_ctis_extractor_api_error(mock_settings: Settings, httpx_mock: HTTPXMock):
    """
    Tests that the CtisExtractor handles an HTTP error gracefully and stops extraction.
    """
    # Mock the search API to return an error on the first page
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        status_code=500,
    )

    extractor = CtisExtractor(settings=mock_settings)
    results = [trial async for trial in extractor.extract_trials()]

    assert len(results) == 0, "Extractor should yield no results on API error."


MOCK_SEARCH_RESPONSE_DELTA = {
    "pagination": {"page": 1, "size": 2, "totalPages": 1, "nextPage": False},
    "data": [
        {"ctNumber": "2022-000004-04", "ctTitle": "Trial 4"},
    ],
}

MOCK_TRIAL_DETAILS_4 = {"ctNumber": "2022-000004-04", "details": "Details for Trial 4"}


@pytest.mark.asyncio
async def test_ctis_extractor_delta_load(mock_settings: Settings, httpx_mock: HTTPXMock):
    """
    Tests that the CtisExtractor can successfully fetch trials for a delta load.
    """
    from_date = "2023-01-01"
    # Mock the search API responses
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json=MOCK_SEARCH_RESPONSE_DELTA,
        match_json={
            "pagination": {"page": 1, "size": 20},
            "sort": {"property": "decisionDate", "direction": "DESC"},
            "advancedSearch": {"decisionDate": {"from": from_date}},
        },
    )

    # Mock the retrieve API responses
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000004-04"),
        json=MOCK_TRIAL_DETAILS_4,
    )

    extractor = CtisExtractor(settings=mock_settings)

    # Run the extraction and collect results
    results = [trial async for trial in extractor.extract_trials(from_decision_date=from_date)]

    # Assertions
    assert len(results) == 1
    assert results[0] == MOCK_TRIAL_DETAILS_4
