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
    proj = submitter_client.add_project( "test project", "Test Description", "a Tagline")

    test_file_1 = os.path.join(testdata_path, "molecule_data", "hooh.json")
    test_file_2 = os.path.join(testdata_path, "molecule_data", "test_archive_1.tar.gz")
    test_file_3 = os.path.join(testdata_path, "molecule_data", "test_archive_1.zip")

    id_1 = proj.upload_attachment(test_file_1, "other", ['tag1'])
    id_2 = proj.upload_attachment(test_file_2, "other", ['tag2'], description="test description", provenance={"test": "test provenance"})
    id_3 = proj.upload_attachment(test_file_3, "other", ['tag3', "tag4"], new_file_name="test_new_name.zip")

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
        source_hash = hashlib.sha256(open(file_path, 'rb').read()).hexdigest()
        assert attach_map[id].sha256sum == source_hash

        # download file
        dest_path = str(tmp_path / f"{id}.data")
        attach_map[id].download(dest_path)

        dest_hash = hashlib.sha256(open(dest_path, 'rb').read()).hexdigest()
        assert source_hash == dest_hash




@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests are not enabled.")
def test_project_client_attachment_delete(submitter_client: PortalClient):
    proj = submitter_client.add_project( "test project", "Test Description", "a Tagline")

    test_file_1 = os.path.join(testdata_path, "molecule_data", "hooh.json")
    test_file_2 = os.path.join(testdata_path, "molecule_data", "test_archive_1.tar.gz")

    id_1 = proj.upload_attachment(test_file_1, "other", ['tag1'])
    id_2 = proj.upload_attachment(test_file_2, "other", ['tag2'], description="test description", provenance={"test": "test provenance"})

    id_map = {id_1: test_file_1, id_2: test_file_2}

    proj = submitter_client.get_project_by_id(proj.id)
    assert len(proj.attachments) == 2

    proj.delete_attachment(id_1)
    assert len(proj.attachments) == 1
    assert proj.attachments[0].id == id_2

