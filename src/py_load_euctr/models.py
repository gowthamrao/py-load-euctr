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
"""Defines the Pydantic data models for the application."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class CtisTrialBronze(BaseModel):
    """Represents a single raw record extracted from the CTIS API.

    Ready to be loaded into the Bronze layer.

    This model includes the complete, unaltered JSON data from the source
    along with essential provenance metadata.
    """

    # Provenance metadata fields as per FRD R.4.2.3.
    # The leading underscore is a database convention; we handle it during loading.
    load_id: str
    extracted_at_utc: datetime
    source_url: str

    # The raw data from the source as per FRD R.4.2.2
    data: dict[str, Any]
