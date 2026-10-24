"""Offline tests for utils/csv_to_ttl.py using a vendored slice of the v14.0.0 model."""
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "utils" / "csv_to_ttl.py"
SLICE = Path(__file__).parent / "fixtures" / "csv_to_ttl" / "mc2_slice.model.csv"

rdflib = pytest.importorskip("rdflib", reason="rdflib is required")
pytest.importorskip("pandas", reason="pandas is required")


def _load_module():
    spec = importlib.util.spec_from_file_location("csv_to_ttl", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def graph(tmp_path_factory):
    out = tmp_path_factory.mktemp("ttl")
    subprocess.run(
        [sys.executable, str(SCRIPT), "-m", str(SLICE), "-g", "mc2", "-o", str(out), "-v", "test"],
        check=True, capture_output=True, text=True,
    )
    ttl = out / "mc2_all_test.ttl"
    assert ttl.exists()
    g = rdflib.Graph()
    g.parse(ttl, format="turtle")
    return g, ttl.read_text()


def test_template_rows_found_via_istemplate(graph):
    g, _ = graph
    subjects = {str(s) for s in g.subjects()}
    # 5 attributes of Biospecimen plus the DUO-dependent userSpecificRestriction row
    assert len(subjects) == 6


def test_prefixes_declared(graph):
    _, text = graph
    assert "@prefix mixs: <https://w3id.org/mixs/> ." in text
    assert "@prefix syn: <http://syn.org/> ." in text  # CDE / CRDC_CDE
    assert "@prefix duo: <http://purl.obolibrary.org/obo/DUO_> ." in text


def test_prefixed_properties_emitted(graph):
    g, _ = graph
    preds = {str(p) for p in g.predicates()}
    assert "http://syn.org/CRDC_CDE" in preds
    assert "https://w3id.org/mixs/MIXS" in preds
    assert "http://purl.obolibrary.org/obo/DUO_DUO" in preds


def test_crdc_cde_not_treated_as_key(graph):
    g, _ = graph
    assert len(list(g.triples((None, rdflib.URIRef("http://syn.org/isKey"), None)))) == 1


def test_get_reference_id_key_classification():
    mod = _load_module()
    refs, keys = mod.get_reference_id("CDE:1, CRDC_CDE:2, primary_key")
    assert refs == "CDE:1, CRDC_CDE:2"
    assert keys == ["primary_key"]
