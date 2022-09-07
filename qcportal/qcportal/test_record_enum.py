import pytest

from qcportal.record_models import RecordStatusEnum, PriorityEnum


def test_record_status_enum():
    assert RecordStatusEnum("running") == RecordStatusEnum.running
    assert RecordStatusEnum("wAItinG") == RecordStatusEnum.waiting
    assert RecordStatusEnum("ERROR") == RecordStatusEnum.error
    assert RecordStatusEnum("iNvAlId") == RecordStatusEnum.invalid
    assert RecordStatusEnum("COMplete") == RecordStatusEnum.complete

    with pytest.raises(ValueError):
        RecordStatusEnum("nvaALId")

    with pytest.raises(ValueError):
        RecordStatusEnum("")


def test_priority_enum():
    assert PriorityEnum("HIGH") == PriorityEnum.high
    assert PriorityEnum("lOw") == PriorityEnum.low
    assert PriorityEnum("normal") == PriorityEnum.normal

    assert PriorityEnum(0) == PriorityEnum.low
    assert PriorityEnum(1) == PriorityEnum.normal
    assert PriorityEnum(2) == PriorityEnum.high

    with pytest.raises(ValueError):
        PriorityEnum(4)

    with pytest.raises(ValueError):
        PriorityEnum("abc")

    with pytest.raises(ValueError):
        PriorityEnum("")
