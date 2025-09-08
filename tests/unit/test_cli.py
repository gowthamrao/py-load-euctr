import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from py_load_euctr.cli import (
    LoadMode,
    _records_to_csv_stream,
    arun_pipeline,
)
from py_load_euctr.models.bronze import BronzeLayerRecord

pytestmark = pytest.mark.unit


# Helper to mock an async generator, as required by the 'async for' loop in the CLI
async def mock_async_gen(data):
    for item in data:
        yield item


def test_records_to_csv_stream():
    """Tests the conversion of records to a CSV byte stream."""
    now = datetime.now(timezone.utc)
    records = [
        BronzeLayerRecord(
            load_id="id1",
            extracted_at_utc=now,
            loaded_at_utc=now,
            source_url="url1",
            package_version="v1",
            record_hash="hash1",
            data={"a": 1},
        ).model_dump(),
    ]

    stream = _records_to_csv_stream(records)
    content = stream.read().decode("utf-8")

    # Assert that the JSON part is correctly quoted by the CSV writer.
    # json.dumps creates '{"a": 1}'. The CSV writer sees the quotes and escapes
    # the whole field, resulting in '"{""a"": 1}"'.
    assert '"{""a"": 1}"' in content


def test_empty_records_to_csv_stream():
    """Tests that an empty list of records produces an empty stream."""
    stream = _records_to_csv_stream([])
    assert stream.read() == b""


@pytest.mark.asyncio
@patch("py_load_euctr.cli.PostgresLoader")
@patch("py_load_euctr.cli.Transformer")
@patch("py_load_euctr.cli.EuctrExtractor")
@patch("py_load_euctr.cli.httpx.AsyncClient")
async def test_arun_pipeline_no_records_found(
    MockAsyncClient, MockExtractor, MockTransformer, MockLoader
):
    """Tests that the pipeline exits gracefully when no records are extracted."""
    # Arrange: Mock the extractor to return an empty async generator
    mock_extractor_instance = MockExtractor.return_value
    mock_extractor_instance.extract_trials.return_value = mock_async_gen([])

    # Act: Run the pipeline, ensuring all optional args have values
    await arun_pipeline(
        load_mode=LoadMode.DELTA, config_file=None, db_dsn_override=None, since_date=None
    )

    # Assert: Check that data loading and transforming methods were not called
    mock_loader_instance = MockLoader.return_value
    mock_transformer_instance = MockTransformer.return_value
    mock_transformer_instance.create_bronze_table.assert_called_once()
    mock_loader_instance.bulk_load_stream.assert_not_called()
    mock_transformer_instance.transform_bronze_to_silver.assert_not_called()


@pytest.mark.asyncio
@patch("py_load_euctr.cli.PostgresLoader")
@patch("py_load_euctr.cli.Transformer")
@patch("py_load_euctr.cli.EuctrExtractor")
@patch("py_load_euctr.cli.httpx.AsyncClient")
async def test_arun_pipeline_with_records(
    MockAsyncClient, MockExtractor, MockTransformer, MockLoader
):
    """Tests the main success path of the pipeline with mocked components."""
    # Arrange: Mock the extractor to return an async generator with one record
    mock_extractor_instance = MockExtractor.return_value
    mock_extractor_instance.extract_trials.return_value = mock_async_gen(
        [{"url": "http://a.b"}]
    )
    mock_loader_instance = MockLoader.return_value
    mock_transformer_instance = MockTransformer.return_value

    # Act: Run the pipeline
    await arun_pipeline(
        load_mode=LoadMode.FULL, config_file=None, db_dsn_override=None, since_date=None
    )

    # Assert: Check that all major steps were called
    mock_transformer_instance.create_bronze_table.assert_called_once()
    mock_loader_instance.bulk_load_stream.assert_called_once()
    mock_transformer_instance.transform_bronze_to_silver.assert_called_once()
