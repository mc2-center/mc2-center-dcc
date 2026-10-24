"""utils/reporter_grants.py, with mocked HTTP.

The expected_* fixtures were produced by the two scripts it replaced
(reporter_project_query.py, clean_reporter_results.py) on the same mocked
response, so these tests keep proving equivalence after those files are gone.
"""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "reporter"

RESULTS = [
    {"project_title": "T1", "project_num": "5U54CA209971-05", "subproject_id": None,
     "fiscal_year": 2021, "project_end_date": "2022-01-01"},
    {"project_title": "T1", "project_num": "5U54CA209971-06", "subproject_id": None,
     "fiscal_year": 2022, "project_end_date": "2023-01-01"},
    {"project_title": "T2", "project_num": "5U54CA214282-03", "subproject_id": "7654",
     "fiscal_year": 2020, "project_end_date": "2021-01-01"},
]


class FakeResponse:
    def __init__(self, status=200, results=None):
        self.status_code = status
        self._results = RESULTS if results is None else results
        self.text = "boom"

    def json(self):
        return {"results": self._results}


def _load(relpath, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def new():
    return _load("utils/reporter_grants.py", "reporter_grants")


def test_payload_matches_old_script(new):
    expected = json.loads((FIXTURES / "expected_payload_ca1_ca2.json").read_text())
    assert new.build_payload(["CA1*", "CA2*"], [2020, 2021], 400) == expected


def test_pipeline_matches_two_step_old_scripts(new, tmp_path, monkeypatch):
    calls = []

    def fake_post(url, headers, data):
        calls.append((url, json.loads(data)))
        return FakeResponse()

    monkeypatch.setattr(new.requests, "post", fake_post)
    monkeypatch.setattr(sys, "argv", ["r", "run", "-g", "CA209971", "CA214282", "-y", "2020",
                                      "-c", str(tmp_path / "new_final.csv")])
    new.main()

    assert (tmp_path / "new_final_raw.csv").read_text() == (FIXTURES / "expected_raw.csv").read_text()
    final = (tmp_path / "new_final.csv").read_text()
    assert final == (FIXTURES / "expected_final.csv").read_text()
    # latest year kept per grant
    assert "5U54CA209971-06" in final and "5U54CA209971-05" not in final
    assert "5U54CA214282-03" in final
    assert calls[0][0] == "https://api.reporter.nih.gov/v2/projects/search"
    assert calls[0][1] == json.loads((FIXTURES / "expected_payload.json").read_text())
    assert calls[0][1]["criteria"]["project_nums"] == ["CA209971*", "CA214282*"]


def test_subcommands_run_separately(new, tmp_path, monkeypatch):
    monkeypatch.setattr(new.requests, "post", lambda **k: FakeResponse())
    raw, final = tmp_path / "raw.csv", tmp_path / "final.csv"
    monkeypatch.setattr(sys, "argv", ["r", "query", "-g", "CA209971", "-c", str(raw)])
    new.main()
    monkeypatch.setattr(sys, "argv", ["r", "clean", "-r", str(raw), "-c", str(final)])
    new.main()
    assert final.exists() and "CA209971" in final.read_text()


def test_non_200_fails_loudly(new, monkeypatch, capsys, tmp_path):
    responses = iter([FakeResponse(status=500)])
    monkeypatch.setattr(new.requests, "post", lambda **k: next(responses))
    with pytest.raises(RuntimeError, match="status 500"):
        new.query_reporter(["CA209971"], [], str(tmp_path / "x.csv"))
    assert "boom" in capsys.readouterr().out
    assert not (tmp_path / "x.csv").exists()


def test_one_failed_chunk_fails_the_whole_query(new, monkeypatch, tmp_path):
    grants = [f"CA{100000 + i}" for i in range(25)]  # > 20 grants: split into chunks
    responses = iter([FakeResponse(), FakeResponse(status=500), FakeResponse()])
    monkeypatch.setattr(new.requests, "post", lambda **k: next(responses))
    with pytest.raises(RuntimeError):
        new.query_reporter(grants, [], str(tmp_path / "x.csv"))
    assert not (tmp_path / "x.csv").exists()
