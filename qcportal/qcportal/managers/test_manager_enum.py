import pytest

from qcportal.managers import ManagerStatusEnum


def test_manager_status_enum():
    assert ManagerStatusEnum("inactive") == ManagerStatusEnum.inactive
    assert ManagerStatusEnum("active") == ManagerStatusEnum.active

    assert ManagerStatusEnum("INACTIVE") == ManagerStatusEnum.inactive
    assert ManagerStatusEnum("INactIVE") == ManagerStatusEnum.inactive
    assert ManagerStatusEnum("acTIVE") == ManagerStatusEnum.active

    with pytest.raises(ValueError):
        ManagerStatusEnum("inactiveabc")

    with pytest.raises(ValueError):
        ManagerStatusEnum("activ")

    with pytest.raises(ValueError):
        ManagerStatusEnum("")
