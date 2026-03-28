from __future__ import annotations

import os
from typing import TYPE_CHECKING

import botocore
import pytest

from qcarchivetesting.helpers import _my_path, s3_tests_enabled
from qcfractal.components.project_db_models import ProjectAttachmentORM
from qcportal.external_files import ExternalFileStatusEnum
from qcportal.project_models import ProjectAttachmentType
from qcportal.record_models import PriorityEnum

test_file_path = os.path.join(_my_path, "molecule_data", "test_archive_1.tar.gz")

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


# Adding/getting is generally tested elsewhere (with various client model tests). But
# a few other core things should be tested

@pytest.mark.skipif(not s3_tests_enabled, reason="S3 tests not enabled")
def test_external_file_socket_delete(storage_socket: SQLAlchemySocket):

    proj_id = storage_socket.projects.add(
        name="test_project",
        description="Test project",
        tagline="Test tagline",
        tags=[],
        default_compute_tag="test_tag",
        default_compute_priority=PriorityEnum.low,
        extras={},
        owner_user=None,
        existing_ok=False,
    )

    ef = ProjectAttachmentORM(
        project_id=proj_id,
        attachment_type=ProjectAttachmentType.other,
        file_name="test_file.tar.gz",
        description="Test file",
        tags=[],
        provenance={"test": "provenance"},
    )

    client = storage_socket.external_files._s3_client

    with storage_socket.session_scope() as session:
        file_id = storage_socket.external_files.add_file(test_file_path, ef, session=session)
        session.commit()

        assert ef.status == ExternalFileStatusEnum.available

        # Check for file presence using the boto3 client
        file_info = client.head_object(Bucket=ef.bucket, Key=ef.object_key)
        assert file_info["ResponseMetadata"]["HTTPStatusCode"] == 200

        session.expunge(ef)

    # Now delete through the socket
    storage_socket.external_files.delete(file_id)

    # File shouldn't exist in the bucket
    with pytest.raises(botocore.exceptions.ClientError):
        client.head_object(Bucket=ef.bucket, Key=ef.object_key)