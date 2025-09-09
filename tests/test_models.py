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

from datetime import datetime
import pytest
from pydantic import ValidationError

from py_load_euctr.models import CtisTrialBronze


def test_ctis_trial_bronze_creation():
    """
    Tests the successful creation of a CtisTrialBronze model.
    """
    now = datetime.utcnow()
    trial_data = {
        "load_id": "test_load_id",
        "extracted_at_utc": now,
        "source_url": "https://example.com/trial/123",
        "data": {"trialId": "123", "title": "Test Trial"},
    }
    bronze_record = CtisTrialBronze(**trial_data)

    assert bronze_record.load_id == "test_load_id"
    assert bronze_record.extracted_at_utc == now
    assert bronze_record.source_url == "https://example.com/trial/123"
    assert bronze_record.data["trialId"] == "123"


def test_ctis_trial_bronze_missing_fields():
    """
    Tests that creating a CtisTrialBronze model with missing
    required fields raises a validation error.
    """
    with pytest.raises(ValidationError):
        CtisTrialBronze(
            load_id="test_load_id",
            # extracted_at_utc is missing
            source_url="https://example.com/trial/123",
            data={},
        )


def test_ctis_trial_bronze_incorrect_types():
    """
    Tests that creating a CtisTrialBronze model with incorrect
    data types for fields raises a validation error.
    """
    with pytest.raises(ValidationError):
        CtisTrialBronze(
            load_id=123,  # Should be a string
            extracted_at_utc="not a datetime",  # Should be a datetime
            source_url="https://example.com/trial/123",
            data={},
        )
