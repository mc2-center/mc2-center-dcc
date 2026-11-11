import pandas as pd
import pytest

import crosswalk as cw
import model_contract as mc
import sync_people as sp
import utils

KNOWN = {"CSBC", "PS-ON", "TEC"}
IDS = {"program.csbc": "CSBC", "program.ps-on": "PS-ON", "program.tec": "TEC"}

GRANTS = pd.DataFrame({"grantNumber": ["CA1", "CA2"], "grantName": ["Grant One", "Grant Two"]})

LIVE = {c: "STRING" for c in cw.expected_columns("PersonView", [])}
LIVE.update({c: "STRING_LIST" for c in ("alternativeNames", "grantName", "grantNumber",
                                        "consortium", "workingGroupParticipation")})


@pytest.fixture(autouse=True)
def fake_lookup(monkeypatch):
    def lookup(syn, ids):
        unknown = [i for i in ids if i not in IDS]
        if unknown:
            raise ValueError(f"Unknown Consortium ID(s) {unknown}")
        return [IDS[i] for i in ids]

    monkeypatch.setattr(utils, "consortium_display_names", lookup)


# (name, grants, consortia (names), publications, profile id)
PEOPLE = [
    ("Ann A", ["CA1"], ["CSBC"], ["PUB1", "PUB2"], "123.0"),
    ("Bob B", ["CA1", "CA2"], ["CSBC", "PS-ON"], [], ""),
    ("Cy C", ["Affiliated/Non-Grant Associated"], ["TEC"], ["PUB3"], ""),
]


def _manifest(legacy: bool, form: str) -> pd.DataFrame:
    def cons(names):
        if form == "id":
            return ["program." + n.lower() for n in names]
        return names

    rows = [
        dict(name=n, alt=[], email=f"{n[0]}@x.org", url="https://x.org", orcid="",
             profile=pid, inst="Inst", wg=["WG1"], chair="", consent="Yes",
             display="True", grants=g, cons=cons(c), pubs=p, ds=[], tools=[])
        for n, g, c, p, pid in PEOPLE
    ]
    df = pd.DataFrame(rows)
    if legacy:
        cols = {
            "component": "PersonView", "personGrantNumber": df.grants,
            "personConsortiumName": df.cons, "name": df.name,
            "alternativeNames": df.alt, "email": df.email, "url": df.url,
            "orcidId": df.orcid, "synapseProfileId": df.profile,
            "lastKnownInstitution": df.inst, "workingGroupParticipation": df.wg,
            "chairRoles": df.chair, "consentForPortalDisplay": df.consent,
            "portalDisplay": df.display, "personPublications": df.pubs,
            "personDatasets": df.ds, "personTools": df.tools,
        }
    else:
        cols = {
            "Component": "PersonView", "PersonView_id": [f"p{i}" for i in range(3)],
            "GrantView Key": df.grants, "Consortium Key": df.cons, "Name": df.name,
            "Alternative Names": df.alt, "Email": df.email, "Url": df.url,
            "Orcid Id": df.orcid, "Synapse Profile Id": df.profile,
            "Last Known Institution": df.inst, "Institution Key": "inst.1",
            "Working Group Participation": df.wg, "Chair Roles": df.chair,
            "Consent For Portal Display": df.consent, "Portal Display": df.display,
            "Person Publications": df.pubs, "Person Datasets": df.ds,
            "Person Tools": df.tools, "Person Educational Resources": [[]] * 3,
        }
        assert list(cols)[:1] == ["Component"]
    return pd.DataFrame(cols)


def _run(manifest, known=KNOWN):
    m = sp.prepare_manifest(manifest)
    consortia = sp.resolve_consortium_keys(None, m["ConsortiumKey"], known_names=known)
    return sp.clean_table(sp.add_missing_info(m, GRANTS, consortia), LIVE)


@pytest.mark.parametrize("form", ["name", "id"])
def test_legacy_and_14_headers_give_identical_output(form):
    legacy = _run(_manifest(True, form))
    new = _run(_manifest(False, form))
    pd.testing.assert_frame_equal(legacy, new)
    assert list(new.columns) == cw.expected_columns("PersonView", [])
    assert "InstitutionKey" not in new.columns


def test_14_manifest_uses_template_headers():
    m = _manifest(False, "id")
    assert set(m.columns) <= set(mc.template_headers("PersonView"))


def test_display_names_and_ids_resolve_identically():
    pd.testing.assert_frame_equal(
        _run(_manifest(True, "name")), _run(_manifest(True, "id"))
    )


def test_values():
    out = _run(_manifest(True, "id"))
    assert out["consortium"].tolist() == [["CSBC"], ["CSBC", "PS-ON"], ["TEC"]]
    assert out["grantNumber"][1] == ["CA1", "CA2"]
    assert out["grantName"].tolist() == [["Grant One"], ["Grant One", "Grant Two"], []]
    # list cells joined with ", " for the STRING columns
    assert out["publicationId"].tolist() == ["PUB1, PUB2", "", "PUB3"]
    assert out["datasetId"].tolist() == ["", "", ""]
    assert out["synapseProfileLink"][0].endswith("Profile:123)")
    assert out["synapseProfileLink"][1] == ""


def test_unknown_values_are_fatal_and_all_reported():
    m = _manifest(True, "name")
    m["personConsortiumName"] = [["Sage Bionetworks"], ["NCI", "CSBC"], ["program.zzz"]]
    with pytest.raises(ValueError) as e:
        _run(m)
    msg = str(e.value)
    assert all(v in msg for v in ("Sage Bionetworks", "NCI", "program.zzz"))
