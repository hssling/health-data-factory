from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_fixed

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchResult:
    local_path: Path
    source_url: str
    fetched_at: str
    not_modified: bool


class ComplianceError(RuntimeError):
    pass


class BaseConnector(ABC):
    name: str = "base"
    description: str = ""
    license: str = ""
    homepage_url: str = ""
    access_type: str = "file"

    def __init__(self, cache_dir: Path, user_agent: str, timeout_seconds: int = 30) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds

    @abstractmethod
    def fetch(
        self, params: dict[str, Any], run_dir: Path, html_allowlist: list[str]
    ) -> FetchResult:
        raise NotImplementedError

    def _cache_prefix(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _headers_for(self, url: str) -> tuple[dict[str, str], Path]:
        prefix = self._cache_prefix(url)
        metadata_path = self.cache_dir / f"{prefix}.meta.json"
        headers = {"User-Agent": self.user_agent}
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            etag = metadata.get("etag")
            modified = metadata.get("last_modified")
            if etag:
                headers["If-None-Match"] = etag
            if modified:
                headers["If-Modified-Since"] = modified
        return headers, metadata_path

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _get(self, url: str, headers: dict[str, str]) -> Response:
        response = requests.get(url, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response

    def _check_html_compliance(self, url: str, html_allowlist: list[str]) -> None:
        domain = urlparse(url).netloc.lower()
        if domain not in html_allowlist:
            raise ComplianceError(f"html source domain not allowlisted: {domain}")
        robots = RobotFileParser()
        robots.set_url(f"{urlparse(url).scheme}://{domain}/robots.txt")
        robots.read()
        if not robots.can_fetch(self.user_agent, url):
            raise ComplianceError(f"robots.txt disallows access for {url}")

    def _write_metadata(self, metadata_path: Path, response: Response) -> None:
        payload = {
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type"),
        }
        metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
