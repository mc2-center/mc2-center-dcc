import pandas as pd

from conftest import load_script

tt = load_script("utils/tally_themes.py", "tt_mod")


def _grants():
    return pd.DataFrame(
        {
            "grantId": ["g1", "g2", "g3"],
            "grantNumber": ["1", "2", "3"],
            "consortium": [["CSBC"], [], ["CSBC", "PS-ON"]],
            "theme": [["T1"], ["T1"], ["T2"]],
        }
    )


def test_grants_without_consortium_are_still_counted():
    """On main an empty consortium list joined to "" and was counted under ""."""
    grants = _grants()
    by_consortium = tt.tally_by_consortium(grants)
    counts = dict(zip(by_consortium["consortium"], by_consortium["totalCount"]))
    assert counts == {"": 1, "CSBC": 1, "CSBC, PS-ON": 1}

    themes = pd.DataFrame(
        {"themeDescription": ["d1", "d2"]}, index=pd.Index(["T1", "T2"], name="theme")
    )
    by_theme = tt.tally_by_theme_consortium(grants, themes)
    rows = {(r.theme, r.consortium): r.totalCount for r in by_theme.itertuples()}
    assert rows == {("T1", ""): 1, ("T1", "CSBC"): 1, ("T2", "CSBC, PS-ON"): 1}
