"""Shared test helpers. Scripts are loaded by path (several share the module
name "utils"), and Synapse is always stubbed."""

import importlib.util
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for sub in ("portal_tables", "annotations"):
    sys.path.insert(0, os.path.join(ROOT, sub))


def load_script(relpath, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, relpath))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
