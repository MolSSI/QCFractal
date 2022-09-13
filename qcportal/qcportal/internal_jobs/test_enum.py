import pytest

from .models import InternalJobStatusEnum


def test_internal_job_status_enum():
    assert InternalJobStatusEnum("running") == InternalJobStatusEnum.running
    assert InternalJobStatusEnum("wAItinG") == InternalJobStatusEnum.waiting
    assert InternalJobStatusEnum("ERROR") == InternalJobStatusEnum.error
    assert InternalJobStatusEnum("COMplete") == InternalJobStatusEnum.complete

    with pytest.raises(ValueError):
        InternalJobStatusEnum("nvaALId")

    with pytest.raises(ValueError):
        InternalJobStatusEnum("")
