import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "portal_tables"))

# Never hit the network: model files come from the vendored v14.0.0 fixtures.
os.environ["MC2_MODEL_DIR"] = str(Path(__file__).parent / "fixtures")


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run tests marked slow")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: benchmark-style tests, skipped unless --runslow is given")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        return
    import pytest

    skip = pytest.mark.skip(reason="slow; use --runslow")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _temp_comma_terms(tmp_path, monkeypatch):
    """Tests read a copy of comma_terms.csv, so learning new terms never edits the real file."""
    import shutil

    import comma_terms

    copy = tmp_path / "comma_terms.csv"
    shutil.copyfile(comma_terms.CSV_PATH, copy)
    monkeypatch.setattr(comma_terms, "CSV_PATH", copy)
    comma_terms._folds.cache_clear()
    yield
    comma_terms._folds.cache_clear()
