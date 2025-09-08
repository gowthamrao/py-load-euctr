from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field


class BronzeLayerRecord(BaseModel):
    """Pydantic model for a single record in the Bronze layer (R.4.2).

    Represents the raw, verbatim data extracted from the source, enriched
    with provenance metadata.
    """

    load_id: str = Field(..., description="Unique identifier for the load execution.")
    extracted_at_utc: datetime = Field(
        ...,
        description="Timestamp (UTC) when the record was retrieved from the source.",
    )
    loaded_at_utc: datetime = Field(
        ...,
        description="Timestamp (UTC) when the record was loaded into the database.",
    )
    source_url: str = Field(
        ..., description="The specific URL the data was extracted from."
    )
    package_version: str = Field(
        ..., description="The version of py-load-euctr used for the load."
    )
    record_hash: str | None = Field(
        default=None, description="SHA-256 hash of the raw record content."
    )
    data: Dict[str, Any] = Field(
        ..., description="The complete raw record structure from the source."
    )
