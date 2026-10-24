from unittest.mock import MagicMock

import pandas as pd
import pytest

import crosswalk as cw
import model_contract as mc
import utils


def _live(component, overrides=None, extra=()):
    """Live column -> type dict as the crosswalk expects it (all STRING)."""
    types = {col: "STRING" for col in cw.expected_columns(component, [])}
    types.update(overrides or {})
    types.update({c: "STRING_LIST" for c in extra})
    return types


# --- source labels -------------------------------------------------------


@pytest.mark.parametrize("component", sorted(cw.CROSSWALK))
def test_every_source_is_a_valid_label_for_its_component(component):
    labels = set(mc.label_map().values())
    # The component's own template is the proof of membership; label_map()
    # also holds enum-value nodes, so membership there alone proves nothing.
    template = mc.normalize_headers(pd.DataFrame(columns=mc.template_headers(component)))
    allowed = set(template.columns)
    for source, portal_col, kind in cw.CROSSWALK[component]:
        assert kind in ("string", "list", "int"), (source, kind)
        if source.startswith("derived:"):
            assert source[len("derived:"):], source
            continue
        assert source in labels or source in mc.PASSTHROUGH, source
        assert source in allowed, f"{source} is not in the {component} template"


@pytest.mark.parametrize("component", sorted(cw.CROSSWALK))
def test_unique_columns(component):
    entries = cw.CROSSWALK[component]
    assert len({e[1] for e in entries}) == len(entries)
    assert len({e[0] for e in entries}) == len(entries)


def test_covers_the_seven_portal_tables():
    assert set(cw.CROSSWALK) == set(cw.COMPONENT_RESOURCE)
    assert len(cw.CROSSWALK) == 7
    for component in cw.CROSSWALK:
        assert cw.table_id(component).startswith("syn")


# --- check_live_columns --------------------------------------------------


def test_check_passes_and_returns_types():
    live = _live("GrantView", {"theme": "STRING_LIST"})
    types = cw.check_live_columns("synX", "GrantView", live)
    assert list(types) == list(live)
    assert types["theme"] == "STRING_LIST"


def test_check_tolerates_trailing_optional_columns():
    live = _live("PersonView")
    live.update({c: "STRING_LIST" for c in utils.OPTIONAL_TRAILING_COLUMNS})
    types = cw.check_live_columns("synX", "PersonView", live)
    assert not set(types) & utils.OPTIONAL_TRAILING_COLUMNS


def test_check_tolerates_missing_optional_and_checks_it_when_present():
    live = _live("ProjectView")
    live.pop("projectShortName", None)
    cw.check_live_columns("synX", "ProjectView", live)
    cw.check_live_columns("synX", "ProjectView", {**live, "projectShortName": "STRING"})
    with pytest.raises(ValueError):  # present but out of position
        cw.check_live_columns(
            "synX", "ProjectView", {"projectShortName": "STRING", **live}
        )


def test_check_rejects_missing_extra_and_reordered():
    live = _live("GrantView")
    short = dict(list(live.items())[:-1])
    with pytest.raises(ValueError, match="crosswalk only"):
        cw.check_live_columns("synX", "GrantView", short)
    with pytest.raises(ValueError, match="live only"):
        cw.check_live_columns("synX", "GrantView", {**live, "surprise": "STRING"})
    keys = list(live)
    keys[0], keys[1] = keys[1], keys[0]
    with pytest.raises(ValueError):
        cw.check_live_columns("synX", "GrantView", {k: "STRING" for k in keys})


def test_unknown_component():
    with pytest.raises(KeyError):
        cw.check_live_columns("synX", "Nope", {})


# --- to_portal -----------------------------------------------------------


def _grant_df(**cols):
    sources = [cw.source_column(s) for s, _, _ in cw.CROSSWALK["GrantView"]]
    df = pd.DataFrame({s: ["x"] for s in sources})
    for k, v in cols.items():
        df[k] = v
    return df


def test_list_column_stays_string_against_string_type():
    live = _live("GrantView")  # theme is STRING
    out = cw.to_portal(_grant_df(GrantThemeName=["a, b"]), "GrantView", live)
    assert out.loc[0, "theme"] == "a, b"


def test_list_column_becomes_list_against_string_list():
    live = _live("GrantView", {"theme": "STRING_LIST"})
    out = cw.to_portal(_grant_df(GrantThemeName=["a, b"]), "GrantView", live)
    assert out.loc[0, "theme"] == ["a", "b"]


def test_existing_lists_pass_through_untouched():
    live = _live("GrantView", {"theme": "STRING_LIST"})
    original = ["a", "b"]
    df = _grant_df()
    df["GrantThemeName"] = pd.Series([original, "c,d"]).iloc[:1].reset_index(drop=True)
    out = cw.to_portal(df, "GrantView", live)
    assert out.loc[0, "theme"] is original

    mixed = pd.concat([df, df], ignore_index=True)
    mixed.at[1, "GrantThemeName"] = "c, d"
    out = cw.to_portal(mixed, "GrantView", live)
    assert out.loc[0, "theme"] is original
    assert out.loc[1, "theme"] == ["c", "d"]


def test_missing_values_pass_through_for_list_columns():
    live = _live("GrantView", {"theme": "STRING_LIST"})
    df = pd.concat([_grant_df(), _grant_df()], ignore_index=True)
    df["GrantThemeName"] = [None, "a"]
    out = cw.to_portal(df, "GrantView", live)
    assert out.loc[0, "theme"] is None
    assert out.loc[1, "theme"] == ["a"]


def test_rename_order_and_drop_extras():
    live = _live("GrantView")
    df = _grant_df(helperColumn=["drop me"])
    out = cw.to_portal(df[reversed(df.columns)], "GrantView", live)
    assert list(out.columns) == list(live)


def test_int_kind_follows_live_type():
    live = _live("GrantView", {"durationOfFunding": "INTEGER"})
    out = cw.to_portal(_grant_df(DurationofFunding=["60"]), "GrantView", live)
    assert out.loc[0, "durationOfFunding"] == 60
    out = cw.to_portal(_grant_df(DurationofFunding=[""]), "GrantView", live)
    assert pd.isna(out.loc[0, "durationOfFunding"])
    with pytest.raises(ValueError):
        cw.to_portal(_grant_df(DurationofFunding=["five"]), "GrantView", live)
    out = cw.to_portal(
        _grant_df(DurationofFunding=["60"]), "GrantView", _live("GrantView")
    )
    assert out.loc[0, "durationOfFunding"] == "60"


def test_missing_source_column_raises():
    df = _grant_df().drop(columns=["GrantName"])
    with pytest.raises(KeyError, match="GrantName"):
        cw.to_portal(df, "GrantView", _live("GrantView"))


def test_optional_column_skipped_while_absent_from_live():
    live = _live("ProjectView")
    live.pop("projectShortName", None)
    sources = [cw.source_column(s) for s, _, _ in cw.CROSSWALK["ProjectView"]]
    df = pd.DataFrame({s: ["x"] for s in sources if s != "ProjectShortName"})
    out = cw.to_portal(df, "ProjectView", live)
    assert "projectShortName" not in out.columns


# --- consortium lookup ---------------------------------------------------


def _consortium_rows(monkeypatch, rows):
    table = MagicMock()
    table.query.return_value = pd.DataFrame(rows)
    monkeypatch.setattr(utils, "Table", MagicMock(return_value=table))
    return table


ROWS = {
    "consortiumName": ["csbc", "pson", "nci_cts"],
    "consortiumDisplayName": [
        "CSBC",
        "PS-ON",
        "NCI Clinical and Translational Exploratory/Developmental Studies",
    ],
}


def test_consortium_table_id_recorded():
    assert utils.CONFIG["consortium"]["table"] == "syn36345930"


def test_consortium_lookup(monkeypatch):
    _consortium_rows(monkeypatch, ROWS)
    out = utils.consortium_display_names(
        None,
        [
            "program.ps-on",
            "program.csbc",
            "program.nci-clinical-and-translational-exploratory-developmental-studies",
        ],
    )
    assert out == [
        "PS-ON",
        "CSBC",
        "NCI Clinical and Translational Exploratory/Developmental Studies",
    ]


def test_consortium_uses_id_column_when_present(monkeypatch):
    _consortium_rows(monkeypatch, {**ROWS, "consortiumId": ["program.a", "program.b", "program.c"]})
    assert utils.consortium_display_names(None, ["program.b"]) == ["PS-ON"]
    with pytest.raises(ValueError):
        utils.consortium_display_names(None, ["program.csbc"])


def test_unknown_program_id_raises(monkeypatch):
    _consortium_rows(monkeypatch, ROWS)
    with pytest.raises(ValueError, match="program.bogus"):
        utils.consortium_display_names(None, ["program.csbc", "program.bogus"])


# --- update_table safety (B0 behaviour) ----------------------------------


def _fake_table(columns, current=None):
    table = MagicMock()
    table.columns = {n: MagicMock(column_type=t) for n, t in columns.items()}
    table.query.return_value = current if current is not None else pd.DataFrame({"a": [1]})
    monkey = MagicMock()
    monkey.return_value.get.return_value = table
    return table, monkey


def _one_row(columns):
    return pd.DataFrame([[""] * len(columns)], columns=columns)


def test_update_table_refuses_empty_frame(monkeypatch):
    live = _live("GrantView")
    table, fake = _fake_table(live)
    monkeypatch.setattr(utils, "Table", fake)
    with pytest.raises(ValueError, match="No rows to store"):
        utils.update_table(None, "synX", pd.DataFrame(columns=list(live)), component="GrantView")
    table.snapshot.assert_not_called()
    table.delete_rows.assert_not_called()


def test_update_table_component_refuses_before_snapshot(monkeypatch):
    live = _live("GrantView")
    table, fake = _fake_table(live)
    monkeypatch.setattr(utils, "Table", fake)
    df = _one_row(list(live)[:-1])  # one column short
    with pytest.raises(ValueError, match="refusing to truncate"):
        utils.update_table(None, "synX", df, component="GrantView")
    table.snapshot.assert_not_called()
    table.delete_rows.assert_not_called()
    table.store_rows.assert_not_called()


def test_update_table_component_refuses_wrong_names_same_count(monkeypatch):
    live = _live("GrantView")
    table, fake = _fake_table(live)
    monkeypatch.setattr(utils, "Table", fake)
    df = _one_row([f"c{i}" for i in range(len(live))])
    with pytest.raises(ValueError, match="refusing to truncate"):
        utils.update_table(None, "synX", df, component="GrantView")
    table.snapshot.assert_not_called()


def test_update_table_component_refuses_live_table_that_drifted(monkeypatch):
    live = {**_live("GrantView"), "surprise": "STRING"}
    table, fake = _fake_table(live)
    monkeypatch.setattr(utils, "Table", fake)
    with pytest.raises(ValueError):
        utils.update_table(None, "synX", _one_row(list(live)), component="GrantView")
    table.snapshot.assert_not_called()


def test_update_table_component_success_and_restore(monkeypatch):
    live = _live("GrantView")
    current = pd.DataFrame({"old": [1, 2]})
    table, fake = _fake_table(live, current)
    monkeypatch.setattr(utils, "Table", fake)
    df = pd.DataFrame([["v"] * len(live)], columns=list(live))
    utils.update_table(None, "synX", df, component="GrantView")
    table.snapshot.assert_called_once()
    table.delete_rows.assert_called_once()
    assert table.store_rows.call_count == 1

    table.reset_mock()
    table.query.return_value = current
    table.store_rows.side_effect = [RuntimeError("rejected"), None]
    with pytest.raises(RuntimeError):
        utils.update_table(None, "synX", df, component="GrantView")
    assert table.store_rows.call_count == 2
    restored = table.store_rows.call_args_list[1].kwargs["values"]
    assert restored.equals(current)


def test_update_table_component_tolerates_trailing_optional(monkeypatch):
    live = _live("PersonView")
    full = {**live, **{c: "STRING_LIST" for c in utils.OPTIONAL_TRAILING_COLUMNS}}
    table, fake = _fake_table(full)
    monkeypatch.setattr(utils, "Table", fake)
    df = pd.DataFrame([["v"] * len(live)], columns=list(live))
    utils.update_table(None, "synX", df, component="PersonView")
    table.snapshot.assert_called_once()


def test_update_table_positional_fallback_unchanged(monkeypatch):
    live = _live("GrantView")
    table, fake = _fake_table(live)
    monkeypatch.setattr(utils, "Table", fake)
    df = pd.DataFrame([["v"] * len(live)], columns=[f"Manifest {i}" for i in range(len(live))])
    utils.update_table(None, "synX", df)
    stored = table.store_rows.call_args.kwargs["values"]
    assert list(stored.columns) == list(live)
    # Still refuses a mismatch before touching the table.
    table.reset_mock()
    with pytest.raises(ValueError, match="refusing to truncate"):
        utils.update_table(None, "synX", _one_row(list("abc")))
    table.snapshot.assert_not_called()
