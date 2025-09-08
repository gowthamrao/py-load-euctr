import asyncio
import logging
import random
from datetime import date
from typing import Any, AsyncGenerator, Dict

import httpx

# A clear User-Agent is required by R.3.4.4
USER_AGENT = "OHDSI/py-load-euctr (v0.1.0; mailto:rao@ohdsi.org)"
# Base URL for the EU CTR portal (placeholder)
BASE_URL = "https://www.clinicaltrialsregister.eu/ctr-search/search"

logger = logging.getLogger(__name__)


class EuctrExtractor:
    """Extractor for fetching clinical trial data from the EU CTR portal (R.3)."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        rate_limit_delay: float = 0.1,  # seconds
        max_retries: int = 3,
    ):
        """Initializes the extractor.

        Args:
            client: An httpx.AsyncClient for making requests.
            rate_limit_delay: Seconds to wait between requests for politeness (R.3.4.3).
            max_retries: Maximum number of retries for a failed request (R.3.4.2).
        """
        self.client = client
        self.client.headers["User-Agent"] = USER_AGENT
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries

    async def _fetch_url(self, url: str, params: Dict | None = None) -> Dict | None:
        """Fetches a single URL with retries and exponential backoff (R.3.4)."""
        for attempt in range(self.max_retries):
            try:
                # Enforce rate limit before the request
                await asyncio.sleep(self.rate_limit_delay)
                response = await self.client.get(url, params=params)
                response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx
                # In a real implementation, we would parse HTML/XML here.
                # For now, we assume the response is JSON.
                return response.json()
            except httpx.HTTPError as e:
                logger.warning(
                    "Request to %s failed on attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self.max_retries,
                    e,
                )
                if attempt + 1 == self.max_retries:
                    logger.error("All retries for %s failed.", url)
                    return None
                # Exponential backoff with jitter
                backoff_time = (2**attempt) + (random.uniform(0, 1))
                await asyncio.sleep(backoff_time)
        return None

    async def extract_trials(
        self, since_date: date | None = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Extracts trial records from the EUCTR portal (R.3.1.1).

        This method handles pagination and fetches trial details concurrently.

        Args:
            since_date: If provided, only fetches trials updated since this date
                        (for delta loads, R.3.2.2).

        Yields:
            A dictionary representing the raw data of a single trial.
        """
        page = 1
        while True:
            # This is a placeholder for the actual search/pagination logic.
            # We assume the API supports pagination and filtering by date.
            query_params: Dict[str, Any] = {"page": page}
            if since_date:
                query_params["last-updated__gte"] = since_date.isoformat()

            search_results = await self._fetch_url(BASE_URL, params=query_params)
            if not search_results or not search_results.get("results"):
                logger.info("No more search results found. Ending extraction.")
                break

            trial_urls = [
                trial["url"] for trial in search_results["results"] if "url" in trial
            ]
            tasks = [self._fetch_url(url) for url in trial_urls]
            for future in asyncio.as_completed(tasks):
                trial_details = await future
                if trial_details:
                    # Here, we would perform Pydantic validation (R.4.5.1)
                    # before yielding the record.
                    yield trial_details

            if not search_results.get("has_next_page"):
                logger.info("Last page of search results reached.")
                break
            page += 1
