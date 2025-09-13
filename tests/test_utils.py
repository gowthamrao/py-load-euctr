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

import datetime
from unittest.mock import MagicMock, patch

import pytest

from py_load_euctr.utils import get_last_decision_date


@pytest.fixture
def mock_postgres_loader():
    """Mocks the PostgresLoader for unit testing."""
    with patch('py_load_euctr.utils.PostgresLoader') as mock_loader:
        # To make the mock loader a context manager, we need to mock __enter__ and __exit__
        mock_instance = MagicMock()
        mock_loader.return_value = mock_instance
        mock_instance.__enter__.return_value = mock_instance
        yield mock_instance


def test_get_last_decision_date_success(mock_postgres_loader):
    """
    Tests that get_last_decision_date returns the correct date string
    when the database call is successful.
    """
    # Arrange: a mock date object is returned by the loader
    mock_date = datetime.date(2024, 1, 15)
    mock_postgres_loader.execute_sql.return_value = (mock_date,)

    # Act
    result = get_last_decision_date("dummy_conn_str", "public", "euctr")

    # Assert
    assert result == "2024-01-15"
    mock_postgres_loader.execute_sql.assert_called_once()


def test_get_last_decision_date_no_data(mock_postgres_loader):
    """
    Tests that get_last_decision_date returns None when the database
    returns no data.
    """
    # Arrange: the loader returns None
    mock_postgres_loader.execute_sql.return_value = None

    # Act
    result = get_last_decision_date("dummy_conn_str", "public", "euctr")

    # Assert
    assert result is None
    mock_postgres_loader.execute_sql.assert_called_once()


def test_get_last_decision_date_exception(mock_postgres_loader):
    """
    Tests that get_last_decision_date returns None when the loader
    raises an exception.
    """
    # Arrange: the loader's method raises an exception
    mock_postgres_loader.execute_sql.side_effect = Exception("DB connection failed")

    # Act
    result = get_last_decision_date("dummy_conn_str", "public", "euctr")

    # Assert
    assert result is None
    mock_postgres_loader.execute_sql.assert_called_once()
