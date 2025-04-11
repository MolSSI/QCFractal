from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pytest

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
