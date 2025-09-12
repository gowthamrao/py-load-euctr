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

"""Tests for the parser module."""

import pytest
from py_load_euctr.parser import parse_trial_to_csv


def test_parse_trial_to_csv_happy_path():
    """
    Tests that a valid trial dictionary is correctly parsed into a CSV string.
    """
    trial_data = {"ctNumber": "2022-500001-01-00", "details": "A trial for a new drug."}
    expected_csv = "2022-500001-01-00,A trial for a new drug.\r\n"
    assert parse_trial_to_csv(trial_data) == expected_csv


def test_parse_trial_to_csv_with_comma():
    """
    Tests that details containing a comma are correctly quoted in the CSV output.
    """
    trial_data = {
        "ctNumber": "2022-500002-02-00",
        "details": "A trial for a new drug, with a comma.",
    }
    expected_csv = '2022-500002-02-00,"A trial for a new drug, with a comma."\r\n'
    assert parse_trial_to_csv(trial_data) == expected_csv


@pytest.mark.parametrize(
    "invalid_input",
    [
        None,
        "a string",
        123,
        [],
        {"ctNumber": "2022-500003-03-00"},  # Missing 'details'
        {"details": "Missing ctNumber"},  # Missing 'ctNumber'
        {"ctNumber": None, "details": "details are here"},
        {"ctNumber": "2022-500004-04-00", "details": None},
        {},
    ],
)
def test_parse_trial_to_csv_invalid_input(invalid_input):
    """
    Tests that the parser returns None for various kinds of invalid or incomplete input.
    """
    assert parse_trial_to_csv(invalid_input) is None


def test_parse_trial_with_special_characters():
    """
    Tests parsing of details with quotes, newlines, and other special characters.
    """
    trial_data = {
        "ctNumber": "2022-500005-05-00",
        "details": 'A trial with "quotes" and a \nnewline.',
    }
    expected_csv = '2022-500005-05-00,"A trial with ""quotes"" and a \nnewline."\r\n'
    assert parse_trial_to_csv(trial_data) == expected_csv
