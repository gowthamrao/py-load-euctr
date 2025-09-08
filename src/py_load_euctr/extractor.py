import httpx
import asyncio
from typing import AsyncGenerator, Dict, Any, Optional

from .config import Settings


class CtisExtractor:
    """
    Extractor for fetching clinical trial data from the CTIS API.
    """

    SEARCH_URL = "https://euclinicaltrials.eu/ctis-public-api/search"
    RETRIEVE_URL_TEMPLATE = "https://euclinicaltrials.eu/ctis-public-api/retrieve/{ct_number}"

    def __init__(self, settings: Settings, client: Optional[httpx.AsyncClient] = None):
        self.settings = settings
        self.client = client if client else httpx.AsyncClient(
            headers={"User-Agent": "py-load-euctr/0.1.0"},
            follow_redirects=True,
            timeout=30.0,
        )

    async def _get_trial_list_page(self, page: int, page_size: int = 20) -> Dict[str, Any]:
        """Fetches a single page of trial search results."""
        # This payload can be extended with search criteria later if needed.
        payload = {
            "pagination": {"page": page, "size": page_size},
            "sort": {"property": "decisionDate", "direction": "DESC"},
        }
        try:
            response = await self.client.post(self.SEARCH_URL, json=payload)
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"Error fetching trial list page {page}: {e!r}")
            return {}

    async def _get_full_trial_details(self, ct_number: str) -> Dict[str, Any]:
        """Fetches the full details for a single clinical trial."""
        url = self.RETRIEVE_URL_TEMPLATE.format(ct_number=ct_number)
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"Error fetching details for trial {ct_number}: {e!r}")
            return {}

    async def extract_trials(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Extracts all clinical trials from the CTIS portal.

        This method handles pagination to retrieve all trial numbers and then
        fetches the full details for each trial. It yields the full JSON data
        for one trial at a time.
        """
        page = 1
        while True:
            search_results = await self._get_trial_list_page(page)
            if not search_results or not search_results.get("data"):
                print("No more trial data found or an error occurred.")
                break

            trial_summaries = search_results.get("data", [])
            ct_numbers = [summary.get("ctNumber") for summary in trial_summaries if summary.get("ctNumber")]

            if not ct_numbers:
                print(f"No trial numbers found on page {page}.")
                break

            # Fetch details concurrently for the current page to improve performance.
            tasks = [self._get_full_trial_details(ct_number) for ct_number in ct_numbers]
            for future in asyncio.as_completed(tasks):
                trial_details = await future
                if trial_details:
                    yield trial_details

            if not search_results.get("pagination", {}).get("nextPage"):
                print("Last page reached.")
                break

            page += 1
