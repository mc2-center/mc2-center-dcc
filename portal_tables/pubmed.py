"""Author lists from PubMed (NCBI E-utilities ``efetch``), read-only.

Used by the grant-table migration to recover ``PublicationAuthors`` cells that an
old export cut off (``...(Read more on Pubmed)``). Names are built like the
curation pipeline's (``"<first name> <last name>"``, e.g. ``"Nathan A Charles"``):
``ForeName LastName`` from the efetch XML, a ``CollectiveName`` as is, in author
order. ``efetch`` is retried up to 3 times (1 s, 4 s, 16 s) on HTTP 429/5xx,
a network error or an XML parse error; if it still fails, the PMIDs of the batch
are reported in ``failed`` and nothing is substituted (``esummary`` only has
short names such as ``"Charles NA"``, which would not match the pipeline).

Requests go out in batches of up to 200 PMIDs with ``tool=mc2-center-dcc`` and
the ``NCBI_API_KEY`` environment variable when set; without a key no more than
3 requests per second are sent (10 with one). Each recovered author list is
cached on disk as ``<cache_dir>/pubmed_efetch/<pmid>.json``; a PMID that PubMed does
not return is not cached.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import xml.etree.ElementTree as ET

import requests

logger = logging.getLogger(__name__)

EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
SOURCE = "pubmed_efetch"
BACKOFF = (1, 4, 16)  # seconds before each retry
BATCH_SIZE = 200
TOOL = "mc2-center-dcc"

_last_request = [0.0]


def _get(url: str, params: dict):
    return requests.get(url, params=params, timeout=60)


class Retryable(Exception):
    """A failure worth another attempt (HTTP 429/5xx, network or XML parse error)."""


def _request(get, url, params, parse, interval, sleep, clock):
    """One rate-limited E-utilities call with retries, parsed to ``{pmid: [author, ...]}``.

    Raises the last error once the retries are used up (or at once for a
    non-retryable status such as 400).
    """
    last = None
    for attempt in range(len(BACKOFF) + 1):
        if attempt:
            sleep(BACKOFF[attempt - 1])
        wait = interval - (clock() - _last_request[0])
        if wait > 0:
            sleep(wait)
        _last_request[0] = clock()
        try:
            response = get(url, params)
        except requests.RequestException as err:
            last = err
            continue
        status = getattr(response, "status_code", 200)
        if status == 429 or status >= 500:
            last = Retryable(f"HTTP {status}")
            continue
        response.raise_for_status()
        try:
            return parse(response)
        except ET.ParseError as err:
            last = err
    raise last


def author_name(author: ET.Element) -> str:
    """``ForeName LastName`` (a missing part is dropped); a ``CollectiveName`` as is."""
    collective = (author.findtext("CollectiveName") or "").strip()
    if collective:
        return collective
    first = (author.findtext("ForeName") or "").strip()
    last = (author.findtext("LastName") or "").strip()
    return f"{first} {last}".strip()


def parse_efetch(response) -> dict[str, list[str]]:
    root = ET.fromstring(response.text)
    out = {}
    for article in root.iter("PubmedArticle"):
        pmid = (article.findtext("MedlineCitation/PMID") or "").strip()
        names = [author_name(a) for a in article.findall("MedlineCitation/Article/AuthorList/Author")]
        out[pmid] = [n for n in names if n]
    return out


def fetch_authors(pmids, cache_dir=None, api_key: str | None = None, get=None, sleep=time.sleep,
                  clock=time.monotonic, failed: set | None = None) -> dict[str, list[str]]:
    """``{pmid: [author, ...]}`` for the PMIDs PubMed returns an author list for.

    A batch that still fails after the retries is logged and its PMIDs are added
    to ``failed`` (a PMID PubMed simply does not return is in neither). ``get``,
    ``sleep`` and ``clock`` are injectable for tests.
    """
    get = get or _get
    api_key = api_key if api_key is not None else os.environ.get("NCBI_API_KEY")
    interval = 0.11 if api_key else 0.34
    found: dict[str, list[str]] = {}
    todo = []
    for pmid in dict.fromkeys(str(p).strip() for p in pmids):
        if not pmid.isdigit():
            continue
        cached = Path(cache_dir) / "pubmed_efetch" / f"{pmid}.json" if cache_dir else None
        if cached is not None and cached.exists():
            found[pmid] = json.loads(cached.read_text())["authors"]
        else:
            todo.append(pmid)
    for start in range(0, len(todo), BATCH_SIZE):
        batch = todo[start:start + BATCH_SIZE]
        params = {"db": "pubmed", "id": ",".join(batch), "retmode": "xml", "tool": TOOL}
        if api_key:
            params["api_key"] = api_key
        try:
            records = _request(get, EFETCH_URL, params, parse_efetch, interval, sleep, clock)
        except Exception:
            logger.exception("PubMed efetch failed for %d PMIDs starting %s; their authors stay truncated",
                             len(batch), batch[0])
            if failed is not None:
                failed.update(batch)
            continue
        for pmid in batch:
            authors = records.get(pmid)
            if not authors:
                continue
            found[pmid] = authors
            if cache_dir:
                path = Path(cache_dir) / "pubmed_efetch" / f"{pmid}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"authors": authors}, ensure_ascii=False))
    return found
