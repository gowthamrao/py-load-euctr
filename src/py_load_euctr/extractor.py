# Copyright 2025 Gowtham Rao <rao@ohdsi.org>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Provides a class to extract clinical trial data from the CTIS API."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

import httpx

from .config import Settings


class CtisExtractor:
    """Extractor for fetching clinical trial data from the CTIS API."""

    SEARCH_URL = "https://euclinicaltrials.eu/ctis-public-api/search"
    RETRIEVE_URL_TEMPLATE = "https://euclinicaltrials.eu/ctis-public-api/retrieve/{ct_number}"

    def __init__(
        self, settings: Settings, client: httpx.AsyncClient | None = None,
    ) -> None:
        """Initialize the extractor with settings and an optional HTTP client."""
        self.settings = settings
        self.client = client or httpx.AsyncClient(
            headers={"User-Agent": "py-load-euctr/0.1.0"},
            follow_redirects=True,
            timeout=30.0,
        )

    async def _get_trial_list_page(
        self, page: int, page_size: int = 20, from_decision_date: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a single page of trial search results."""
        payload = {
            "pagination": {"page": page, "size": page_size},
            "sort": {"property": "decisionDate", "direction": "DESC"},
        }
        # Add the decision date filter if provided.
        # This is based on an educated guess of the API's capabilities.
        if from_decision_date:
            payload["advancedSearch"] = {"decisionDate": {"from": from_decision_date}}

        try:
            response = await self.client.post(self.SEARCH_URL, json=payload)
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.error("Failed to fetch trial list page: %s", e)
            return {}

    async def _get_full_trial_details(self, ct_number: str) -> dict[str, Any]:
        """Fetch the full details for a single clinical trial."""
        url = self.RETRIEVE_URL_TEMPLATE.format(ct_number=ct_number)
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    async def extract_trials(
        self, from_decision_date: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Extract clinical trials from the CTIS portal.

        If `from_decision_date` is provided (in YYYY-MM-DD format), it fetches
        trials with a decision date after that date (inclusive). Otherwise, it
        fetches all trials.

        This method handles pagination and fetches full details for each trial.
        It yields the full JSON data for one trial at a time.
        """
        page = 1
        while True:
            search_results = await self._get_trial_list_page(
                page, from_decision_date=from_decision_date,
            )
            if not search_results or not search_results.get("data"):
                break

            trial_summaries = search_results.get("data", [])
            ct_numbers = [
                summary.get("ctNumber")
                for summary in trial_summaries
                if summary.get("ctNumber")
            ]

            if not ct_numbers:
                break

            # Fetch details concurrently for the current page to improve performance.
            tasks = [
                self._get_full_trial_details(ct_number) for ct_number in ct_numbers
            ]
            for future in asyncio.as_completed(tasks):
                try:
                    trial_details = await future
                    if trial_details:
                        yield trial_details
                except (httpx.RequestError, httpx.HTTPStatusError) as e:
                    logging.warning("Skipping trial due to error: %s", e)

            if not search_results.get("pagination", {}).get("nextPage"):
                break

            page += 1
