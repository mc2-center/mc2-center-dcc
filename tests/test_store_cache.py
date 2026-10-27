"""store_new_version must not poison the client's cache; read_recordset checks the md5 (stubs only)."""
import asyncio
import hashlib
from types import SimpleNamespace
from unittest import mock

import pytest
from synapseclient.models import RecordSet
from synapseclient.models.file import _needs_upload
from synapseclient.models.file import FileHandle

import recordsets as rs


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def fetched_entity(new_path):
    """What ``RecordSet(...).get(download_file=False)`` returns: the old file handle, then a new local path."""
    entity = RecordSet(
        id="syn1", parent_id="syn2", download_file=False, data_file_handle_id="111", content_size=10,
        file_handle=FileHandle(id="111", content_md5=md5(b"old")),
    )
    entity._set_last_persistent_instance()
    entity.path = str(new_path)
    return entity


class FakeCache:
    def __init__(self):
        self.added = []

    def contains(self, file_handle_id, path):
        return False

    def add(self, file_handle_id, path, md5=None):
        self.added.append((file_handle_id, path))


def test_upload_check_poisons_the_cache_with_the_old_handle_unless_the_handle_is_cleared(tmp_path):
    new = tmp_path / "removal.csv"
    new.write_text("new contents")
    syn = SimpleNamespace(cache=FakeCache())
    # the synapseclient behaviour behind the bug: old handle id mapped to the new file
    assert asyncio.run(_needs_upload(syn=syn, entity_to_upload=fetched_entity(new)))
    assert syn.cache.added == [("111", str(new))]
    # with the handle fields cleared, as store_new_version does, nothing is added
    syn = SimpleNamespace(cache=FakeCache())
    entity = fetched_entity(new)
    for field in ("file_handle", "data_file_handle_id", "content_md5", "content_size"):
        setattr(entity, field, None)
    assert asyncio.run(_needs_upload(syn=syn, entity_to_upload=entity))
    assert syn.cache.added == []


def test_store_new_version_clears_the_old_file_handle_before_storing(tmp_path):
    seen = {}
    entity = fetched_entity(tmp_path / "x.csv")

    def store(synapse_client=None):
        seen.update({f: getattr(entity, f) for f in
                     ("file_handle", "data_file_handle_id", "content_md5", "content_size", "path")})
        return SimpleNamespace(version_number=8)

    entity.store = store
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value = entity
        assert rs.store_new_version("syn", "syn1", tmp_path / "x.csv") == 8
    assert seen == {"file_handle": None, "data_file_handle_id": None, "content_md5": None,
                    "content_size": None, "path": str(tmp_path / "x.csv")}
    assert entity.id == "syn1" and entity.parent_id == "syn2"  # identity and binding fields are untouched


def entity_with(path, content):
    return SimpleNamespace(path=str(path), file_handle=SimpleNamespace(content_md5=md5(content)))


def test_read_recordset_downloads_again_when_the_cached_file_has_the_wrong_md5(tmp_path):
    wrong = tmp_path / "wrong.csv"
    wrong.write_text("k\nA\n")  # v6's contents served for v5
    right_dir = tmp_path / "dl"
    right_dir.mkdir()
    right = right_dir / "right.csv"
    right.write_text("k\nA\nB\n")
    calls = []

    def make(**kw):
        calls.append(kw)
        entity = entity_with(right if "download_location" in kw else wrong, b"k\nA\nB\n")
        return SimpleNamespace(get=lambda synapse_client=None: entity)

    with mock.patch.object(rs, "RecordSet", side_effect=make):
        df = rs.read_recordset("syn", "syn1", version=5, cache_dir=tmp_path / "cache")
    assert list(df["k"]) == ["A", "B"]
    assert len(calls) == 2 and "download_location" in calls[1] and calls[1]["version_number"] == 5
    assert (tmp_path / "cache" / "syn1" / "v5.csv").read_text() == "k\nA\nB\n"  # the cache gets the right file
    assert not (tmp_path / "wrong.csv.part").exists()


def test_read_recordset_with_a_matching_md5_downloads_once(tmp_path):
    good = tmp_path / "good.csv"
    good.write_text("k\nA\n")
    calls = []

    def make(**kw):
        calls.append(kw)
        return SimpleNamespace(get=lambda synapse_client=None: entity_with(good, b"k\nA\n"))

    with mock.patch.object(rs, "RecordSet", side_effect=make):
        assert list(rs.read_recordset("syn", "syn1", version=5)["k"]) == ["A"]
    assert len(calls) == 1


def test_read_recordset_raises_when_the_fresh_download_is_wrong_too(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("k\nZ\n")
    with mock.patch.object(rs, "RecordSet", side_effect=lambda **kw: SimpleNamespace(
            get=lambda synapse_client=None: entity_with(bad, b"something else"))):
        with pytest.raises(ValueError, match="md5"):
            rs.read_recordset("syn", "syn1", version=5, cache_dir=tmp_path / "cache")
    assert not (tmp_path / "cache" / "syn1" / "v5.csv").exists()
