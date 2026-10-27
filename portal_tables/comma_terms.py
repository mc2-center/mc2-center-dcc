"""Known list items that contain ", " (MeSH names such as ``Aged, 80 and over``).

portal_tables/comma_terms.csv is seeded by utils/sync_comma_terms.py and grows
as uploads bring new terms. It is only consulted where a list cell exists as
comma-joined text; JSON-array cells carry their items intact.
"""

import csv
from datetime import date
from functools import lru_cache
from pathlib import Path

import utils

CSV_PATH = Path(__file__).with_name("comma_terms.csv")
COLUMNS = ["term", "source", "added"]


def load_terms(path=None) -> list[dict[str, str]]:
    """The rows of the terms CSV, in file order (none if the file is absent)."""
    path = Path(path or CSV_PATH)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@lru_cache(maxsize=None)
def _folds(path) -> tuple[frozenset[str], int]:
    """Case-folded terms of the CSV at ``path`` and the most pieces any of them splits into."""
    terms = [r["term"] for r in load_terms(path)]
    return frozenset(" ".join(t.split()).casefold() for t in terms), max((t.count(", ") + 1 for t in terms), default=1)


def is_keyword_column(column: str) -> bool:
    """Whether ``column`` (class label) holds free-text keywords such as MeSH terms."""
    return column.endswith("Keywords")


def group_pieces(pieces: list[str], path=None) -> list[list[str]]:
    """Group comma-split ``pieces``: at each position the longest run whose ", "-join is a known term."""
    known, longest = _folds(str(path or CSV_PATH))
    groups, i = [], 0
    while i < len(pieces):
        for n in range(min(longest, len(pieces) - i), 1, -1):
            if " ".join(", ".join(pieces[i : i + n]).split()).casefold() in known:
                break
        else:
            n = 1
        groups.append(pieces[i : i + n])
        i += n
    return groups


def split_keyword_cell(value, rejoined: list | None = None, path=None) -> list[str]:
    """A keyword cell as items: a JSON array as written, comma text with known terms kept whole.

    The pieces of each rejoined term are appended to ``rejoined`` when given.
    """
    items = utils.split_cell(value)
    if isinstance(value, (list, tuple)) or (isinstance(value, str) and utils.parse_json_list(value) is not None):
        return items
    out = []
    for group in group_pieces(items, path):
        if len(group) > 1 and rejoined is not None:
            rejoined.append(group)
        out.append(", ".join(group))
    return out


def learn(items, source: str, path=None) -> list[str]:
    """Append the items holding ", " that the CSV does not know (case-folded); return those added.

    Existing rows are neither reordered nor rewritten, so running twice adds nothing.
    """
    path = Path(path or CSV_PATH)
    known = set(_folds(str(path))[0])
    new = []
    for item in items:
        fold = " ".join(item.split()).casefold()
        if ", " in item and fold not in known:
            known.add(fold)
            new.append(item)
    if new:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, lineterminator="\n")
            if not text:
                writer.writerow(COLUMNS)
            elif not text.endswith("\n"):
                f.write("\n")
            writer.writerows([t, source, date.today().isoformat()] for t in new)
        _folds.cache_clear()
    return new
