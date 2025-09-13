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

import asyncio
import pytest

from py_load_euctr.config import Settings
from py_load_euctr.extractor import CtisExtractor


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ctis_extractor_fetches_and_parses_trials():
    """
    A more robust integration test for the CtisExtractor.

    This test:
    1. Initializes a CtisExtractor.
    2. Fetches a small number of trials from the live API.
    3. Verifies that the trials have the expected structure.
    """
    settings = Settings()
    extractor = CtisExtractor(settings)

    # Fetch the first 2 trials from the beginning of 2023
    # Using a fixed date makes the test more deterministic
    from_decision_date = "2023-01-01"
    trials_generator = extractor.extract_trials(from_decision_date=from_decision_date)

    trials = []
    try:
        for _ in range(2):
            trial = await anext(trials_generator)
            trials.append(trial)
    except StopAsyncIteration:
        pass

    assert len(trials) > 0
    if len(trials) > 2:
        trials = trials[:2]

    assert len(trials) <= 2

    for trial in trials:
        # These are a smaller set of keys that we expect to be present in
        # every trial record. Some keys are optional in the API response.
        expected_keys = [
            "ctNumber",
            "ctStatus",
            "authorizedApplication",
        ]
        for key in expected_keys:
            assert key in trial, f"Expected key '{key}' not in trial"

        # Also check a few nested keys to be more thorough
        part_i = trial.get("authorizedApplication", {}).get("authorizedPartI", {})
        assert "trialDetails" in part_i, "'trialDetails' not in 'authorizedPartI'"

        clinical_trial_identifiers = part_i.get("trialDetails", {}).get(
            "clinicalTrialIdentifiers", {}
        )
        assert (
            "publicTitle" in clinical_trial_identifiers
        ), "'publicTitle' not in 'clinicalTrialIdentifiers'"


# Helper to get the next item from an async generator
async def anext(async_generator):
    return await async_generator.__anext__()
