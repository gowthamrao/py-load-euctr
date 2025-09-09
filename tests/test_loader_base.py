# Copyright 2025 Gowtham Rao <rao@ohdsi.org>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import io
from py_load_euctr.loader.base import BaseLoader


# A minimal concrete class for testing the abstract base class
class MinimalLoader(BaseLoader):
    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return super().__exit__(exc_type, exc_val, exc_tb)

    def bulk_load_stream(self, target_table, data_stream, columns=None, delimiter=","):
        return super().bulk_load_stream(target_table, data_stream, columns, delimiter)

    def execute_sql(self, sql, params=None):
        return super().execute_sql(sql, params)


@pytest.fixture
def minimal_loader():
    """Provides an instance of MinimalLoader."""
    return MinimalLoader()


def test_base_loader_enter_raises_not_implemented(minimal_loader):
    """
    Tests that calling __enter__ on a class that hasn't implemented it
    raises NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        with minimal_loader:
            pass


def test_base_loader_exit_raises_not_implemented(minimal_loader):
    """
    Tests that calling __exit__ on a class that hasn't implemented it
    raises NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        minimal_loader.__exit__(None, None, None)


def test_base_loader_bulk_load_stream_raises_not_implemented(minimal_loader):
    """
    Tests that calling bulk_load_stream on a class that hasn't implemented it
    raises NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        minimal_loader.bulk_load_stream("a", io.BytesIO(b"c"))


def test_base_loader_execute_sql_raises_not_implemented(minimal_loader):
    """
    Tests that calling execute_sql on a class that hasn't implemented it
    raises NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        minimal_loader.execute_sql("a")
