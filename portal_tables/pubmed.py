"""Author lists from PubMed (NCBI E-utilities ``esummary``), read-only.

Used by the grant-table migration to recover ``PublicationAuthors`` cells that an
old export cut off (``...(Read more on Pubmed)``). Authors come back as PubMed's
``name`` field (``"Charles NA"``), in author order.

Requests go out in batches of up to 200 PMIDs with ``tool=mc2-center-dcc`` and
the ``NCBI_API_KEY`` environment variable when set; without a key no more than
3 requests per second are sent (10 with one). Each recovered author list is
cached on disk as ``<cache_dir>/pubmed/<pmid>.json``; a PMID that PubMed does
not return is not cached.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
BATCH_SIZE = 200
TOOL = "mc2-center-dcc"

_last_request = [0.0]


def _get(url: str, params: dict):
    return requests.get(url, params=params, timeout=60)


def fetch_authors(pmids, cache_dir=None, api_key: str | None = None, get=None, sleep=time.sleep,
                  clock=time.monotonic) -> dict[str, list[str]]:
    """``{pmid: [author, ...]}`` for the PMIDs PubMed returns an author list for.

    A batch that fails is logged and its PMIDs are left out (the caller keeps
    the original value). ``get``, ``sleep`` and ``clock`` are injectable for tests.
    """
    get = get or _get
    api_key = api_key if api_key is not None else os.environ.get("NCBI_API_KEY")
    interval = 0.11 if api_key else 0.34
    found: dict[str, list[str]] = {}
    todo = []
    for pmid in dict.fromkeys(str(p).strip() for p in pmids):
        if not pmid.isdigit():
            continue
        cached = Path(cache_dir) / "pubmed" / f"{pmid}.json" if cache_dir else None
        if cached is not None and cached.exists():
            found[pmid] = json.loads(cached.read_text())["authors"]
        else:
            todo.append(pmid)
    for start in range(0, len(todo), BATCH_SIZE):
        batch = todo[start:start + BATCH_SIZE]
        params = {"db": "pubmed", "id": ",".join(batch), "retmode": "json", "tool": TOOL}
        if api_key:
            params["api_key"] = api_key
        wait = interval - (clock() - _last_request[0])
        if wait > 0:
            sleep(wait)
        _last_request[0] = clock()
        try:
            response = get(ESUMMARY_URL, params)
            response.raise_for_status()
            result = response.json()["result"]
        except Exception:
            logger.exception("PubMed esummary failed for %d PMIDs starting %s", len(batch), batch[0])
            continue
        for pmid in batch:
            record = result.get(pmid)
            authors = [a["name"] for a in (record or {}).get("authors", []) if a.get("name")]
            if not authors:
                continue
            found[pmid] = authors
            if cache_dir:
                path = Path(cache_dir) / "pubmed" / f"{pmid}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"authors": authors}, ensure_ascii=False))
    return found
