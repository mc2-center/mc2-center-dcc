"""Read one list cell (a manifest, RecordSet or portal cell that holds several values).

A cell is a list, a JSON array (``["A","B"]``), comma text (``A, B``) or empty.
"""

import json

import pandas as pd

PENDING = "Pending Annotation"
"""The placeholder for a value that is not yet annotated."""


def is_null(value) -> bool:
    """Whether ``value`` is ``None`` or a missing scalar (NaN, ``pd.NA``, ``NaT``). A list or a string is not."""
    return value is None or (pd.api.types.is_scalar(value) and bool(pd.isna(value)))


def json_list(text: str) -> list | None:
    """The list a JSON-array cell holds, or ``None`` if ``text`` is not one."""
    stripped = text.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            parsed = json.loads(stripped)
        except ValueError:
            return None
        if isinstance(parsed, list):
            return parsed
    return None


def parse_list(value, group=None, rejoined: list | None = None) -> list[str]:
    """A list cell as a list of stripped, non-empty strings; a null cell is ``[]``.

    A list, tuple or set gives its items. A JSON array gives its items. Other text is split on commas.
    ``group`` (for example ``comma_terms.group_pieces``) takes the comma pieces and returns them grouped, so that
    a known term that holds a comma stays one item; the pieces of each rejoined term are appended to ``rejoined``.
    Items of a list or JSON array are not grouped: they carry their commas intact.
    """
    if isinstance(value, (list, tuple, set)):
        return _clean(value)
    if is_null(value):
        return []
    text = str(value)
    parsed = json_list(text)
    if parsed is not None:
        return _clean(parsed)
    items = _clean(text.split(","))
    if group is None:
        return items
    out = []
    for pieces in group(items):
        if len(pieces) > 1 and rejoined is not None:
            rejoined.append(pieces)
        out.append(", ".join(pieces))
    return out


def first_item(value) -> str:
    """The first item of a list cell (see :func:`parse_list`), or ``""`` when it has none."""
    return (parse_list(value) or [""])[0]


def _clean(items) -> list[str]:
    return [s for s in (str(i).strip() for i in items) if s]


def is_placeholder(value) -> bool:
    """Whether every item of the cell is ``PENDING`` (case and surrounding space ignored). A null or blank cell is not."""
    items = parse_list(value)
    return bool(items) and all(i.strip().casefold() == PENDING.casefold() for i in items)
