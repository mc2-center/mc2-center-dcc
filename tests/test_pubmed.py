"""portal_tables/pubmed.py with the HTTP calls mocked."""
import json
from xml.sax.saxutils import escape

import pubmed


class Resp:
    def __init__(self, text="", result=None):
        self.text = text
        self._result = result

    def raise_for_status(self):
        pass

    def json(self):
        return {"result": self._result}


def author_xml(a):
    if isinstance(a, str):
        return f"<Author><CollectiveName>{escape(a)}</CollectiveName></Author>"
    last, fore = a
    out = f"<LastName>{escape(last)}</LastName>" if last else ""
    out += f"<ForeName>{escape(fore)}</ForeName><Initials>X</Initials>" if fore else ""
    return f"<Author>{out}</Author>"


def efetch_xml(known, ids):
    articles = "".join(
        f"<PubmedArticle><MedlineCitation><PMID>{i}</PMID><Article><AuthorList>"
        + "".join(author_xml(a) for a in known[i]) + "</AuthorList></Article></MedlineCitation></PubmedArticle>"
        for i in ids if i in known
    )
    return f"<?xml version='1.0'?><PubmedArticleSet>{articles}</PubmedArticleSet>"


def eutils(known, short=None, efetch_fails=False):
    """A fake E-utilities: efetch XML from ``known`` (PMID -> [(last, fore) | collective]); esummary from ``short``."""
    calls = []

    def get(url, params):
        calls.append((url.rsplit("/", 1)[1], params))
        ids = params["id"].split(",")
        if url.endswith("efetch.fcgi"):
            if efetch_fails:
                raise RuntimeError("503")
            return Resp(text=efetch_xml(known, ids))
        result = {"uids": ids}
        for i in ids:
            result[i] = {"uid": i, "authors": [{"name": n} for n in (short or {}).get(i, [])]}
        return Resp(result=result)

    return get, calls


def test_full_names_are_forename_then_lastname_in_author_order(monkeypatch):
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    get, calls = eutils({"1": [("Charles", "Nathan A"), ("Holland", "Eric C")]})
    out = pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None)
    assert out == {"1": ["Nathan A Charles", "Eric C Holland"]}
    name, params = calls[0]
    assert name == "efetch.fcgi"
    assert params == {"db": "pubmed", "id": "1", "retmode": "xml", "tool": "mc2-center-dcc"}


def test_collective_names_are_used_as_is_and_missing_forenames_dropped():
    get, _ = eutils({"1": [("Smith", "Jo"), "The ABC Consortium", ("Madonna", None)]})
    assert pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None) == {
        "1": ["Jo Smith", "The ABC Consortium", "Madonna"]}


def test_non_ascii_names_survive():
    get, _ = eutils({"1": [("Jovanović", "Bojana")]})
    assert pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None) == {"1": ["Bojana Jovanović"]}


def test_a_pmid_pubmed_does_not_return_is_left_out():
    get, _ = eutils({"1": [("B", "A")]})
    assert pubmed.fetch_authors(["1", "2", "not-a-pmid"], get=get, sleep=lambda s: None) == {"1": ["A B"]}


def test_batches_of_200_and_deduplicated_ids():
    known = {str(i): [("L", f"F{i}")] for i in range(1, 451)}
    get, calls = eutils(known)
    out = pubmed.fetch_authors([str(i) for i in range(1, 451)] + ["1", "2"], get=get, sleep=lambda s: None)
    assert len(out) == 450
    assert [len(c[1]["id"].split(",")) for c in calls] == [200, 200, 50]


def test_api_key_is_sent_when_set(monkeypatch):
    get, calls = eutils({"1": [("B", "A")]})
    pubmed.fetch_authors(["1"], api_key="KEY", get=get, sleep=lambda s: None)
    assert calls[0][1]["api_key"] == "KEY"
    monkeypatch.setenv("NCBI_API_KEY", "ENVKEY")
    get, calls = eutils({"1": [("B", "A")]})
    pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None)
    assert calls[0][1]["api_key"] == "ENVKEY"


def test_requests_are_spaced_at_three_per_second_without_a_key(monkeypatch):
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    now = [100.0]
    waits = []

    def sleep(s):
        waits.append(round(s, 3))
        now[0] += s

    get, _ = eutils({str(i): [("L", "F")] for i in range(1, 401)})
    pubmed._last_request[0] = 0.0
    pubmed.fetch_authors([str(i) for i in range(1, 401)], get=get, sleep=sleep, clock=lambda: now[0])
    assert waits == [0.34]


def test_responses_are_cached_on_disk_and_misses_are_not(tmp_path):
    get, _ = eutils({"1": [("Charles", "Nathan A")]})
    pubmed.fetch_authors(["1", "2"], cache_dir=tmp_path, get=get, sleep=lambda s: None)
    assert json.loads((tmp_path / "pubmed_efetch" / "1.json").read_text()) == {"authors": ["Nathan A Charles"]}
    assert not (tmp_path / "pubmed_efetch" / "2.json").exists()
    get2, calls2 = eutils({"1": [("X", "changed")], "2": [("Add", "Late")]})
    out = pubmed.fetch_authors(["1", "2"], cache_dir=tmp_path, get=get2, sleep=lambda s: None)
    assert out == {"1": ["Nathan A Charles"], "2": ["Late Add"]}
    assert calls2[0][1]["id"] == "2"


def test_esummary_is_the_logged_fallback_when_efetch_fails(tmp_path, caplog):
    get, calls = eutils({}, short={"1": ["Charles NA", "Holland EC"]}, efetch_fails=True)
    out = pubmed.fetch_authors(["1"], cache_dir=tmp_path, get=get, sleep=lambda s: None)
    assert out == {"1": ["Charles NA", "Holland EC"]}
    assert [c[0] for c in calls] == ["efetch.fcgi", "esummary.fcgi"]
    assert calls[1][1]["retmode"] == "json"
    assert "falling back to esummary" in caplog.text
    assert not (tmp_path / "pubmed_efetch").exists()  # short names are not cached


def test_a_batch_failing_on_both_endpoints_is_skipped_not_raised():
    def get(url, params):
        raise RuntimeError("503")

    assert pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None) == {}
