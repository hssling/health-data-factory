from __future__ import annotations

from pydantic import BaseModel


class DatasetSummary(BaseModel):
    id: str
    title: str
    description: str
    refresh_cron: str
