from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pytest

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcportal.record_models import PriorityEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


@pytest.mark.parametrize(
    "default_tag,default_priority,default_user,default_group",
    [
        ("*", PriorityEnum.low, None, None),
        ("a_tag", PriorityEnum.high, "admin_user", "group1"),
        ("TAG2", PriorityEnum.normal, "submit_user", None),
    ],
)
def test_dataset_socket_submit_defaults(
    secure_snowflake: QCATestingSnowflake,
    default_tag: str,
    default_priority: PriorityEnum,
    default_user: Optional[str],
    default_group: Optional[str],
):
    storage_socket = secure_snowflake.get_storage_socket()

    default_user_id = storage_socket.users.get_optional_user_id(default_user)
    group1_id = storage_socket.groups.get("group1")["id"]

    ds_id = storage_socket.datasets.singlepoint.add(
        name="Test SP Dataset",
        description="",
        tagline="",
        tags=[],
        provenance={},
        default_compute_tag=default_tag,
        default_compute_priority=default_priority,
        extras={},
        creator_user=default_user,
        existing_ok=False,
    )

    ds = storage_socket.datasets.get(ds_id)
    assert ds["default_tag"] == default_tag.lower()
    assert ds["default_priority"] == default_priority
    assert ds["owner_user"] == default_user

    # creator_user is not part of the wire model output (yet)
    assert "creator_user" not in ds

    # but creator_user_id is set at the ORM level, immutably mirroring owner_user_id
    with storage_socket.session_scope() as session:
        ds_orm = session.get(BaseDatasetORM, ds_id)
        assert ds_orm.creator_user_id == default_user_id
        assert ds_orm.creator_user_id == ds_orm.owner_user_id

    tag, priority = storage_socket.datasets.singlepoint.get_submit_defaults(ds_id)
    assert tag == default_tag.lower()
    assert priority == default_priority

    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(
        ds_id,
        None,
        None,
        None,
    )
    assert tag == default_tag.lower()
    assert priority == default_priority
    assert user_id is None

    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(ds_id, None, None, default_user)
    assert tag == default_tag.lower()
    assert priority == default_priority
    assert user_id == default_user_id

    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(ds_id, None, None, "submit_user")
    assert tag == default_tag.lower()
    assert priority == default_priority

    # No user = no group either
    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(ds_id, "diFFerent_TAG", None, None)
    assert tag == "different_tag"
    assert priority == default_priority
    assert user_id is None

    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(
        ds_id, "diFFerent_TAG", None, default_user
    )
    assert tag == "different_tag"
    assert priority == default_priority
    assert user_id == default_user_id

    tag, priority, user_id = storage_socket.datasets.singlepoint.get_submit_info(
        ds_id, "different_tag", PriorityEnum.normal, default_user
    )
    assert tag == "different_tag"
    assert priority == PriorityEnum.normal
    assert user_id == default_user_id


@pytest.mark.parametrize("creator_user", [None, "admin_user", "submit_user"])
def test_dataset_socket_clone_creator(secure_snowflake: QCATestingSnowflake, creator_user: Optional[str]):
    storage_socket = secure_snowflake.get_storage_socket()

    ds_id = storage_socket.datasets.singlepoint.add(
        name="Test SP Dataset",
        description="",
        tagline="",
        tags=[],
        provenance={},
        default_compute_tag="*",
        default_compute_priority=PriorityEnum.normal,
        extras={},
        creator_user="submit_user",
        existing_ok=False,
    )

    new_ds_id = storage_socket.datasets.singlepoint.clone(ds_id, "Cloned SP Dataset", creator_user)
    assert new_ds_id != ds_id

    # The clone is created (and owned) by the user doing the cloning, not the source's creator
    ds = storage_socket.datasets.get(new_ds_id)
    assert ds["owner_user"] == creator_user

    source_ds = storage_socket.datasets.get(ds_id)
    assert source_ds["owner_user"] == "submit_user"
