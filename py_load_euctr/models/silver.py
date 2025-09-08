from datetime import date

from pydantic import BaseModel


class Trial(BaseModel):
    """Pydantic model for the core Trial entity in the Silver layer (R.4.3.2).

    Represents a cleansed, normalized view of a clinical trial.
    """

    trial_id: str  # Renamed from id to avoid shadowing a Python builtin
    title: str
    protocol_id: str | None = None
    status: str
    start_date: date | None = None
    end_date: date | None = None
