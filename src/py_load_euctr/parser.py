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

"""Contains functions for parsing data from various sources.

Such as cleaning up raw JSON from APIs or scraping HTML.
"""

import csv
import io
from typing import Any


def parse_trial_to_csv(trial_data: dict[str, Any]) -> str | None:
    """
    Parses a single trial's JSON data into a CSV-formatted string.

    Args:
        trial_data: A dictionary containing the trial data from the API.

    Returns:
        A string containing a single CSV row, or None if the input is invalid.
    """
    if not isinstance(trial_data, dict):
        return None

    ct_number = trial_data.get("ctNumber")
    details = trial_data.get("details")

    if not ct_number or not details:
        return None

    # Use StringIO to build the CSV row in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([ct_number, details])

    # Return the content of the StringIO object
    return output.getvalue()
