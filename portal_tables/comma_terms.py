"""Known list items that contain ", " (MeSH names such as ``Aged, 80 and over``).

portal_tables/comma_terms.csv is seeded by utils/sync_comma_terms.py and grows
as uploads bring new terms. It is only consulted where a list cell exists as
comma-joined text; JSON-array cells carry their items intact.
"""

import csv
from pathlib import Path

CSV_PATH = Path(__file__).with_name("comma_terms.csv")
COLUMNS = ["term", "source", "added"]


def load_terms(path=None) -> list[dict[str, str]]:
    """The rows of the terms CSV, in file order (none if the file is absent)."""
    path = Path(path or CSV_PATH)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
