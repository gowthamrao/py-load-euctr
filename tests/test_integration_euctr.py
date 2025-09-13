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

import pytest
import httpx


@pytest.mark.integration
def test_euctr_api_search_endpoint():
    """
    Tests that the EUCTR API search endpoint returns a successful response.
    This is a live integration test and requires an internet connection.
    """
    url = "https://euclinicaltrials.eu/ctis-public-api/search"
    payload = {
        "pagination": {"page": 1, "size": 1},
        "sort": {"property": "decisionDate", "direction": "DESC"},
    }
    with httpx.Client() as client:
        response = client.post(url, json=payload)
        assert response.status_code == 200
        response_json = response.json()
        assert "data" in response_json
        assert "pagination" in response_json
        assert isinstance(response_json["data"], list)
        assert isinstance(response_json["pagination"], dict)
