from unittest.mock import MagicMock, patch

import pytest

from py_load_euctr.loaders.base import BaseLoader
from py_load_euctr.transformers.euctr import Transformer

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_loader():
    """Provides a MagicMock of the BaseLoader for dependency injection."""
    return MagicMock(spec=BaseLoader)


@patch("py_load_euctr.transformers.euctr.importlib.resources.path")
@patch("py_load_euctr.transformers.euctr.Environment")
def test_transformer_init(mock_jinja_env, mock_resources_path, mock_loader):
    """Tests that the transformer initializes the Jinja2 environment correctly."""
    # The importlib.resources.path is a context manager, so we mock its enter value
    mock_resources_path.return_value.__enter__.return_value = "/fake/path/to/sql"

    Transformer(loader=mock_loader)

    # Assert that the Jinja2 Environment was instantiated
    mock_jinja_env.assert_called_once()
    # Check that the loader was a FileSystemLoader pointing to the correct path
    loader_arg = mock_jinja_env.call_args[1]["loader"]
    assert "FileSystemLoader" in str(loader_arg)
    assert "/fake/path/to/sql" in str(loader_arg.searchpath)


def test_transformer_create_bronze_table(mock_loader):
    """Tests that the correct template is rendered and executed for creating bronze table."""
    transformer = Transformer(loader=mock_loader)
    # Replace the transformer's jinja_env with a mock to control its behavior
    mock_jinja_env = MagicMock()
    transformer.jinja_env = mock_jinja_env
    mock_template = mock_jinja_env.get_template.return_value

    mock_conn = MagicMock()
    transformer.create_bronze_table(mock_conn, schema="raw", table="trials")

    # Assert the correct template file was requested
    mock_jinja_env.get_template.assert_called_with("bronze/create_bronze_table.sql")
    # Assert the template was rendered with the correct context
    mock_template.render.assert_called_with(schema="raw", table="trials")
    # Assert the loader was called with the rendered SQL and connection
    mock_loader.execute_sql.assert_called_with(
        mock_template.render.return_value, mock_conn
    )


def test_transformer_transform_bronze_to_silver(mock_loader):
    """Tests that the bronze-to-silver transformation is called correctly."""
    transformer = Transformer(loader=mock_loader)
    mock_jinja_env = MagicMock()
    transformer.jinja_env = mock_jinja_env
    mock_template = mock_jinja_env.get_template.return_value

    mock_conn = MagicMock()
    transformer.transform_bronze_to_silver(
        conn=mock_conn,
        bronze_schema="raw",
        bronze_table="euctr_trials",
        silver_schema="silver",
        load_id="test-load-id",
    )

    # Assert the correct template file was requested
    mock_jinja_env.get_template.assert_called_with("silver/upsert_trials.sql")
    # Assert the template was rendered with the correct context
    mock_template.render.assert_called_with(
        bronze_schema="raw",
        bronze_table="euctr_trials",
        silver_schema="silver",
        load_id="test-load-id",
    )
    # Assert the loader was called with the rendered SQL
    mock_loader.execute_sql.assert_called_with(
        mock_template.render.return_value, mock_conn
    )
