"""portal_tables/pubmed.py with the HTTP calls mocked."""
import json

import pubmed


class Resp:
    def __init__(self, result):
        self._result = result

    def raise_for_status(self):
        pass

    def json(self):
        return {"result": self._result}


def esummary(known):
    """A fake esummary: ``known`` maps PMID -> author names; others come back with an error record."""
    calls = []

    def get(url, params):
        calls.append((url, params))
        ids = params["id"].split(",")
        result = {"uids": [i for i in ids if i in known]}
        for i in ids:
            result[i] = ({"uid": i, "authors": [{"name": n, "authtype": "Author"} for n in known[i]]}
                         if i in known else {"uid": i, "error": "cannot get document summary"})
        return Resp(result)

    return get, calls


def test_authors_come_back_in_pubmed_order_with_tool_and_json_params(monkeypatch):
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    get, calls = esummary({"1": ["Charles NA", "Holland EC"]})
    out = pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None)
    assert out == {"1": ["Charles NA", "Holland EC"]}
    url, params = calls[0]
    assert url.endswith("esummary.fcgi")
    assert params == {"db": "pubmed", "id": "1", "retmode": "json", "tool": "mc2-center-dcc"}


def test_a_pmid_pubmed_does_not_return_is_left_out():
    get, _ = esummary({"1": ["A B"]})
    assert pubmed.fetch_authors(["1", "2", "not-a-pmid"], get=get, sleep=lambda s: None) == {"1": ["A B"]}


def test_batches_of_200_and_deduplicated_ids():
    known = {str(i): [f"Author{i}"] for i in range(1, 451)}
    get, calls = esummary(known)
    out = pubmed.fetch_authors([str(i) for i in range(1, 451)] + ["1", "2"], get=get, sleep=lambda s: None)
    assert len(out) == 450
    assert [len(c[1]["id"].split(",")) for c in calls] == [200, 200, 50]


def test_api_key_is_sent_when_set_and_changes_the_rate(monkeypatch):
    get, calls = esummary({"1": ["A B"]})
    pubmed.fetch_authors(["1"], api_key="KEY", get=get, sleep=lambda s: None)
    assert calls[0][1]["api_key"] == "KEY"
    monkeypatch.setenv("NCBI_API_KEY", "ENVKEY")
    get, calls = esummary({"1": ["A B"]})
    pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None)
    assert calls[0][1]["api_key"] == "ENVKEY"


def test_requests_are_spaced_at_three_per_second_without_a_key(monkeypatch):
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    now = [100.0]
    waits = []

    def sleep(s):
        waits.append(round(s, 3))
        now[0] += s

    known = {str(i): ["A"] for i in range(1, 401)}
    get, _ = esummary(known)
    pubmed._last_request[0] = 0.0
    pubmed.fetch_authors([str(i) for i in range(1, 401)], get=get, sleep=sleep, clock=lambda: now[0])
    assert waits == [0.34]  # two batches: the second waits out the interval, the first (long ago) does not


def test_responses_are_cached_on_disk_and_misses_are_not(tmp_path):
    get, calls = esummary({"1": ["Charles NA"]})
    pubmed.fetch_authors(["1", "2"], cache_dir=tmp_path, get=get, sleep=lambda s: None)
    assert json.loads((tmp_path / "pubmed" / "1.json").read_text()) == {"authors": ["Charles NA"]}
    assert not (tmp_path / "pubmed" / "2.json").exists()
    get2, calls2 = esummary({"1": ["changed"], "2": ["Late Add"]})
    out = pubmed.fetch_authors(["1", "2"], cache_dir=tmp_path, get=get2, sleep=lambda s: None)
    assert out == {"1": ["Charles NA"], "2": ["Late Add"]}
    assert calls2[0][1]["id"] == "2"  # 1 came from the cache


def test_a_failing_batch_is_skipped_not_raised():
    def get(url, params):
        raise RuntimeError("503")

    assert pubmed.fetch_authors(["1"], get=get, sleep=lambda s: None) == {}
