"""Author lists from PubMed (NCBI E-utilities ``efetch``), read-only.

Used by the grant-table migration to recover ``PublicationAuthors`` cells that an
old export cut off (``...(Read more on Pubmed)``). Names are built like the
curation pipeline's (``"<first name> <last name>"``, e.g. ``"Nathan A Charles"``):
``ForeName LastName`` from the efetch XML, a ``CollectiveName`` as is, in author
order. If ``efetch`` fails for a batch, ``esummary`` is the logged fallback; its
names are PubMed's short form (``"Charles NA"``).

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
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
BATCH_SIZE = 200
TOOL = "mc2-center-dcc"

_last_request = [0.0]


def _get(url: str, params: dict):
    return requests.get(url, params=params, timeout=60)


def _request(get, url, params, parse, interval, sleep, clock):
    """One rate-limited E-utilities call, parsed to ``{pmid: [author, ...]}``."""
    wait = interval - (clock() - _last_request[0])
    if wait > 0:
        sleep(wait)
    _last_request[0] = clock()
    response = get(url, params)
    response.raise_for_status()
    return parse(response)


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


def parse_esummary(response) -> dict[str, list[str]]:
    result = response.json()["result"]
    return {pmid: [a["name"] for a in record.get("authors", []) if a.get("name")]
            for pmid, record in result.items() if isinstance(record, dict) and pmid != "uids"}


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
        fallback = False
        try:
            records = _request(get, EFETCH_URL, params, parse_efetch, interval, sleep, clock)
        except Exception:
            logger.exception("PubMed efetch failed for %d PMIDs starting %s; falling back to esummary (short names)",
                             len(batch), batch[0])
            params = {**params, "retmode": "json"}
            fallback = True
            try:
                records = _request(get, ESUMMARY_URL, params, parse_esummary, interval, sleep, clock)
            except Exception:
                logger.exception("PubMed esummary failed for %d PMIDs starting %s", len(batch), batch[0])
                continue
        for pmid in batch:
            authors = records.get(pmid)
            if not authors:
                continue
            found[pmid] = authors
            if cache_dir and not fallback:  # short names are not cached
                path = Path(cache_dir) / "pubmed_efetch" / f"{pmid}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"authors": authors}, ensure_ascii=False))
    return found
