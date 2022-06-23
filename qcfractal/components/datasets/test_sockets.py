from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.records import PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


@pytest.mark.parametrize(
    "default_tag,default_priority",
    [
        ("*", PriorityEnum.low),
        ("a_tag", PriorityEnum.high),
        ("TAG2", PriorityEnum.normal),
    ],
)
def test_dataset_socket_default_tag_priority(
    storage_socket: SQLAlchemySocket, default_tag: str, default_priority: PriorityEnum
):

    ds_id = storage_socket.datasets.singlepoint.add(
        name="Test SP Dataset",
        description="",
        tagline="",
        tags=[],
        group="default",
        provenance={},
        visibility=True,
        default_tag=default_tag,
        default_priority=default_priority,
        metadata={},
    )

    ds = storage_socket.datasets.get(ds_id)
    assert ds["default_tag"] == default_tag.lower()
    assert ds["default_priority"] == default_priority

    tag, priority = storage_socket.datasets.singlepoint.get_default_tag_priority(ds_id)
    assert tag == default_tag.lower()
    assert priority == default_priority

    tag, priority = storage_socket.datasets.singlepoint.get_tag_priority(ds_id, None, None)
    assert tag == default_tag.lower()
    assert priority == default_priority

    tag, priority = storage_socket.datasets.singlepoint.get_tag_priority(ds_id, "diFFerent_TAG", None)
    assert tag == "different_tag"
    assert priority == default_priority

    tag, priority = storage_socket.datasets.singlepoint.get_tag_priority(ds_id, "different_tag", PriorityEnum.normal)
    assert tag == "different_tag"
    assert priority == PriorityEnum.normal
