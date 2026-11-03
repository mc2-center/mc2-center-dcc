"""Local jsonschema validation against the 14.x data-model JSON Schemas.

Shared by ``annotations/migrate_grant_tables_14.py`` (I5a dry runs) and
``portal_tables/union_qc.py`` (I6 merged manifests). The schemas come from
``model_contract.fetch`` at a data-models tag (``--schema-version``). Frames use
class-label headers; list columns hold Python lists.

author: orion.banks
"""

import json
import logging
import re
import urllib.error
from functools import lru_cache

import jsonschema
import pandas as pd

import model_contract

logger = logging.getLogger(__name__)

VALIDATION_COLUMNS = ["key", "field", "validator", "message"]

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:\S+$")
_REQUIRED = re.compile(r"^'(.+)' is a required property$")


def _format_checker() -> jsonschema.FormatChecker:
    checker = jsonschema.FormatChecker(formats=[])
    checker.checks("uri")(lambda value: not isinstance(value, str) or bool(_URI.match(value)))
    return checker


def _instance(row: dict, properties: dict) -> dict:
    """A row as the JSON object Synapse validates: blank cells absent, numbers numeric."""
    out = {}
    for column, value in row.items():
        if value == "" or value == []:
            continue
        kind = properties.get(column, {}).get("type")
        if kind in ("number", "integer") and isinstance(value, str):
            try:
                value = float(value) if kind == "number" else int(value)
            except ValueError:
                pass  # left as text: the type error is the finding
        out[column] = value
    return out


@lru_cache(maxsize=None)
def load_schema(component: str, ref: str | None = None) -> dict:
    """``json_schemas/<component>.json`` at the data-models tag ``ref`` (e.g. ``v14.0.1``).

    An unavailable tag logs a warning and falls back to ``MODEL_REF``'s schema.
    """
    try:
        path = model_contract.fetch(f"json_schemas/{component}.json", ref=ref)
    except (urllib.error.URLError, FileNotFoundError) as err:
        logger.warning("Schema %s at data-models %s is not available (%s); validating against %s",
                       component, ref, err, model_contract.MODEL_REF)
        path = model_contract.fetch(f"json_schemas/{component}.json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def validate_frame(frame: pd.DataFrame, component: str, ref: str | None = None, key: str | None = None) -> pd.DataFrame:
    """Local jsonschema failures: ``key, field, validator, message``.

    The schema is the one at data-models tag ``ref`` (``--schema-version``).
    ``key`` is the column holding each row's identifier (default ``<component>Id``).

    Array cells are the lists in ``frame`` (a string holds one string, as
    ``GrantViewKey`` does). Blank cells are absent, so ``required`` fails for them.
    """
    schema = load_schema(component, ref)
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema, format_checker=_format_checker())
    key = key or f"{component}Id"
    rows = []
    for record in frame.to_dict("records"):
        for error in sorted(validator.iter_errors(_instance(record, schema["properties"])), key=lambda e: list(map(str, e.path))):
            if error.path:
                field = str(error.path[0])
            else:
                match = _REQUIRED.match(error.message)
                field = match.group(1) if match else ""
            rows.append({"key": record[key], "field": field, "validator": error.validator, "message": error.message})
    return pd.DataFrame(rows, columns=VALIDATION_COLUMNS)


def validation_counts(validation: pd.DataFrame) -> pd.DataFrame:
    """Failures counted by field and validator, with one example message."""
    if validation.empty:
        return pd.DataFrame(columns=["field", "validator", "count", "example"])
    counts = (
        validation.groupby(["field", "validator"])
        .agg(count=("key", "size"), example=("message", "first"))
        .reset_index()
        .sort_values(["count", "field"], ascending=[False, True])
    )
    return counts
