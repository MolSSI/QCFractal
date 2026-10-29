from __future__ import annotations

import hashlib
import os
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting.helpers import s3_tests_enabled, _my_path as testdata_path

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_add_get(submitter_client: PortalClient, tmp_path):
    proj = submitter_client.add_project("test project", "Test Description", "a Tagline")

    test_file_1 = os.path.join(testdata_path, "molecule_data", "hooh.json")
    test_file_2 = os.path.join(testdata_path, "molecule_data", "test_archive_1.tar.gz")
    test_file_3 = os.path.join(testdata_path, "molecule_data", "test_archive_1.zip")

    id_1 = proj.upload_attachment(test_file_1, "other", ["tag1"])
    id_2 = proj.upload_attachment(
        test_file_2, "other", ["tag2"], description="test description", provenance={"test": "test provenance"}
    )
    id_3 = proj.upload_attachment(test_file_3, "other", ["tag3", "tag4"], new_file_name="test_new_name.zip")

    id_map = {id_1: test_file_1, id_2: test_file_2, id_3: test_file_3}

    proj = submitter_client.get_project_by_id(proj.id)
    assert len(proj.attachments) == 3

    attach_map = {a.id: a for a in proj.attachments}
    assert attach_map[id_1].file_name == "hooh.json"
    assert attach_map[id_1].description == ""
    assert attach_map[id_1].provenance == {}

    assert attach_map[id_2].file_name == "test_archive_1.tar.gz"
    assert attach_map[id_2].description == "test description"
    assert attach_map[id_2].provenance == {"test": "test provenance"}

    assert attach_map[id_3].file_name == "test_new_name.zip"
    assert attach_map[id_3].description == ""
    assert attach_map[id_3].provenance == {}

    for id, file_path in id_map.items():
        # check file hashes
        source_hash = hashlib.sha256(open(file_path, "rb").read()).hexdigest()
        assert attach_map[id].sha256sum == source_hash

        # download file
        dest_path = str(tmp_path / f"{id}.data")
        attach_map[id].download(dest_path)

        dest_hash = hashlib.sha256(open(dest_path, "rb").read()).hexdigest()
        assert source_hash == dest_hash


@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_delete(submitter_client: PortalClient):
    proj = submitter_client.add_project("test project", "Test Description", "a Tagline")

    test_file_1 = os.path.join(testdata_path, "molecule_data", "hooh.json")
    test_file_2 = os.path.join(testdata_path, "molecule_data", "test_archive_1.tar.gz")

    id_1 = proj.upload_attachment(test_file_1, "other", ["tag1"])
    id_2 = proj.upload_attachment(
        test_file_2, "other", ["tag2"], description="test description", provenance={"test": "test provenance"}
    )

    id_map = {id_1: test_file_1, id_2: test_file_2}

    proj = submitter_client.get_project_by_id(proj.id)
    assert len(proj.attachments) == 2

    proj.delete_attachment(id_1)
    assert len(proj.attachments) == 1
    assert proj.attachments[0].id == id_2


@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_download(submitter_client: PortalClient, tmp_path):
    proj = submitter_client.add_project("test project", "Test Description", "a Tagline")

    test_file = os.path.join(testdata_path, "molecule_data", "hooh.json")
    att_id = proj.upload_attachment(test_file, "other", ["tag1"])

    source_bytes = open(test_file, "rb").read()

    # Explicit destination; returns the path
    dest_path = str(tmp_path / "downloaded.json")
    assert proj.download_attachment(att_id, dest_path) == dest_path
    assert open(dest_path, "rb").read() == source_bytes

    # Default destination is the current directory, with the stored file name
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        assert proj.download_attachment(att_id) == os.path.join(str(tmp_path), "hooh.json")
    finally:
        os.chdir(cwd)
    assert open(tmp_path / "hooh.json", "rb").read() == source_bytes

    # Works on a freshly-fetched project, too
    proj2 = submitter_client.get_project_by_id(proj.id)
    dest_path_2 = str(tmp_path / "downloaded2.json")
    proj2.download_attachment(att_id, dest_path_2)
    assert open(dest_path_2, "rb").read() == source_bytes


@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_download_overwrite(submitter_client: PortalClient, tmp_path):
    proj = submitter_client.add_project("test project", "Test Description", "a Tagline")

    test_file = os.path.join(testdata_path, "molecule_data", "hooh.json")
    att_id = proj.upload_attachment(test_file, "other", ["tag1"])
    source_bytes = open(test_file, "rb").read()

    dest_path = tmp_path / "existing.json"
    dest_path.write_bytes(b"existing")

    with pytest.raises(RuntimeError, match="already exists"):
        proj.download_attachment(att_id, str(dest_path))
    assert dest_path.read_bytes() == b"existing"

    proj.download_attachment(att_id, str(dest_path), overwrite=True)
    assert dest_path.read_bytes() == source_bytes


@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_download_invalid(submitter_client: PortalClient, tmp_path):
    proj = submitter_client.add_project("test project", "Test Description", "a Tagline")

    test_file = os.path.join(testdata_path, "molecule_data", "hooh.json")
    att_id = proj.upload_attachment(test_file, "other", ["tag1"])

    dest_path = tmp_path / "nope.json"
    with pytest.raises(ValueError, match=rf"not a valid attachment.*\[{att_id}\]"):
        proj.download_attachment(att_id + 1000, str(dest_path))
    assert not dest_path.exists()

    # Attachment of another project is not valid
    proj_other = submitter_client.add_project("other project", "Test Description", "a Tagline")
    with pytest.raises(ValueError, match="not a valid attachment"):
        proj_other.download_attachment(att_id, str(dest_path))
