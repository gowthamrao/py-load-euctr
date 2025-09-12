# Copyright 2025 Gowtham Rao <rao@ohdsi.org>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may not obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


@pytest.mark.asyncio
async def test_ctis_extractor_no_trials_found(
    mock_settings: Settings, httpx_mock: HTTPXMock
):
    """
    Tests that the extractor handles the case where the search returns no trials.
    """
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={"pagination": {"page": 1, "size": 20, "totalPages": 0, "nextPage": False}, "data": []},
    )

    extractor = CtisExtractor(settings=mock_settings)
    results = [trial async for trial in extractor.extract_trials()]

    assert len(results) == 0


@pytest.mark.asyncio
async def test_ctis_extractor_timeout_error(
    mock_settings: Settings, httpx_mock: HTTPXMock
):
    """
    Tests that the extractor can handle a timeout error and stop processing.
    """
    httpx_mock.add_exception(
        httpx.TimeoutException("Timeout"),
        method="POST",
        url=CtisExtractor.SEARCH_URL
    )

    extractor = CtisExtractor(settings=mock_settings)
    results = [trial async for trial in extractor.extract_trials()]

    assert len(results) == 0


@pytest.mark.asyncio
async def test_ctis_extractor_retrieve_error(
    mock_settings: Settings, httpx_mock: HTTPXMock
):
    """
    Tests that the extractor can handle an error when retrieving a single trial
    and continue processing other trials.
    """
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 2, "totalPages": 1, "nextPage": False},
            "data": [
                {"ctNumber": "2022-000001-01", "ctTitle": "Trial 1"},
                {"ctNumber": "2022-000002-02", "ctTitle": "Trial 2"},
            ],
        },
    )

    # Mock the first retrieve call to fail
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000001-01"),
        status_code=500,
    )
    # Mock the second retrieve call to succeed
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-000002-02"),
        json=MOCK_TRIAL_DETAILS_2,
    )

    extractor = CtisExtractor(settings=mock_settings)
    results = [trial async for trial in extractor.extract_trials()]

    assert len(results) == 1
    assert results[0] == MOCK_TRIAL_DETAILS_2


@pytest.mark.asyncio
async def test_ctis_extractor_trial_with_no_ct_number(
    mock_settings: Settings, httpx_mock: HTTPXMock
):
    """
    Tests that the extractor can handle trials that are missing a ctNumber
    and stops processing further pages.
    """
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 20, "totalPages": 2, "nextPage": True},
            "data": [{"ctTitle": "Trial with no number"}]  # Missing ctNumber
        },
    )

    extractor = CtisExtractor(settings=mock_settings)
    results = [trial async for trial in extractor.extract_trials()]

    assert len(results) == 0
