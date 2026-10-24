"""utils/reporter_grants.py against the two scripts it replaces, with mocked HTTP."""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

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
    old = _load("utils/reporter_project_query.py", "old_query")
    args = (["CA1*", "CA2*"], [2020, 2021], 400)
    assert new.build_payload(*args) == old.build_payload(*args)


def test_pipeline_matches_two_step_old_scripts(new, tmp_path, monkeypatch, capsys):
    old_q = _load("utils/reporter_project_query.py", "old_query")
    old_c = _load("utils/clean_reporter_results.py", "old_clean")
    calls = []

    def fake_post(url, headers, data):
        calls.append((url, json.loads(data)))
        return FakeResponse()

    monkeypatch.setattr(new.requests, "post", fake_post)
    monkeypatch.setattr(old_q.requests, "post", fake_post)

    # old two-step
    monkeypatch.setattr(sys, "argv", ["q", "-g", "CA209971", "CA214282", "-y", "2020", "-c",
                                      str(tmp_path / "old_raw.csv")])
    old_q.main()
    monkeypatch.setattr(sys, "argv", ["c", "-r", str(tmp_path / "old_raw.csv"), "-c",
                                      str(tmp_path / "old_final.csv")])
    old_c.main()

    # new single pipeline
    monkeypatch.setattr(sys, "argv", ["r", "run", "-g", "CA209971", "CA214282", "-y", "2020",
                                      "-c", str(tmp_path / "new_final.csv")])
    new.main()

    assert (tmp_path / "new_final_raw.csv").read_text() == (tmp_path / "old_raw.csv").read_text()
    final = (tmp_path / "new_final.csv").read_text()
    assert final == (tmp_path / "old_final.csv").read_text()
    # latest year kept per grant
    assert "5U54CA209971-06" in final and "5U54CA209971-05" not in final
    assert "5U54CA214282-03" in final
    assert calls[0][0] == calls[1][0] == "https://api.reporter.nih.gov/v2/projects/search"
    assert calls[0][1] == calls[1][1]
    assert calls[0][1]["criteria"]["project_nums"] == ["CA209971*", "CA214282*"]


def test_subcommands_run_separately(new, tmp_path, monkeypatch):
    monkeypatch.setattr(new.requests, "post", lambda **k: FakeResponse())
    raw, final = tmp_path / "raw.csv", tmp_path / "final.csv"
    monkeypatch.setattr(sys, "argv", ["r", "query", "-g", "CA209971", "-c", str(raw)])
    new.main()
    monkeypatch.setattr(sys, "argv", ["r", "clean", "-r", str(raw), "-c", str(final)])
    new.main()
    assert final.exists() and "CA209971" in final.read_text()


def test_non_200_reports_response_text(new, monkeypatch, capsys, tmp_path):
    responses = iter([FakeResponse(status=500)])
    monkeypatch.setattr(new.requests, "post", lambda **k: next(responses))
    with pytest.raises(ValueError):  # nothing to concatenate, as in the old script
        new.query_reporter(["CA209971"], [], str(tmp_path / "x.csv"))
    assert "boom" in capsys.readouterr().out
