from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any


class CtisTrialBronze(BaseModel):
    """
    Represents a single raw record extracted from the CTIS API,
    ready to be loaded into the Bronze layer.

    This model includes the complete, unaltered JSON data from the source
    along with essential provenance metadata.
    """
    # Provenance metadata fields as per FRD R.4.2.3.
    # The leading underscore is a database convention; we handle it during loading.
    load_id: str
    extracted_at_utc: datetime
    source_url: str

    # The raw data from the source as per FRD R.4.2.2
    data: Dict[str, Any]
