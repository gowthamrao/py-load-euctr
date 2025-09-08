from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from py_load_euctr.models.bronze import BronzeLayerRecord
from py_load_euctr.models.silver import Trial

pytestmark = pytest.mark.unit


def test_bronze_layer_record_creation():
    """Tests successful creation of a BronzeLayerRecord."""
    now = datetime.now(timezone.utc)
    record = BronzeLayerRecord(
        load_id="test_id",
        extracted_at_utc=now,
        loaded_at_utc=now,
        source_url="http://test.com",
        package_version="0.1.0",
        record_hash="some_hash",
        data={"key": "value"},
    )
    assert record.load_id == "test_id"
    assert record.data["key"] == "value"
    assert record.record_hash == "some_hash"


def test_bronze_layer_record_missing_required_field():
    """Tests that a validation error is raised for missing required fields."""
    with pytest.raises(ValidationError):
        BronzeLayerRecord(
            load_id="test_id",
            # Missing other required fields like extracted_at_utc, data, etc.
        )


def test_trial_model_creation():
    """Tests successful creation of a Silver layer Trial model."""
    trial = Trial(
        trial_id="test_trial_123",
        title="A great trial",
        status="Ongoing",
    )
    assert trial.trial_id == "test_trial_123"
    assert trial.status == "Ongoing"


def test_trial_model_optional_fields():
    """Tests that optional fields can be None."""
    trial = Trial(
        trial_id="test_trial_456",
        title="Another trial",
        status="Completed",
        protocol_id=None,
        start_date=None,
        end_date=None,
    )
    assert trial.protocol_id is None
    assert trial.start_date is None
    assert trial.end_date is None
