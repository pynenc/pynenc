import pytest

from pynenc.conf.config_base import ConfigBase, ConfigField, ConfigMultiInheritanceError


class ConfigParent0(ConfigBase):
    unique_0 = ConfigField(default_value=0)
    common = ConfigField(0)


class ConfigParent1(ConfigBase):
    unique_1 = ConfigField(1)
    common = ConfigField(1)


class ConfigParent2(ConfigBase):
    unique_2 = ConfigField(2)


class ConfigChildConflict(ConfigParent0, ConfigParent1):
    unique_child = ConfigField("child-conflict")


class ConfigChildOk(ConfigParent0, ConfigParent2):
    unique_child = ConfigField("child-ok")


class ConfigChildMulti(ConfigChildOk, ConfigParent1):
    ...


def test_avoid_parent_same_config() -> None:
    """Test that a child with 2 parents cannot have same config field in both parents"""
    with pytest.raises(ConfigMultiInheritanceError) as exc_info:
        _ = ConfigChildConflict()
    assert "common" in str(exc_info.value)
    assert ConfigParent0.__name__ in str(exc_info.value)
    assert ConfigParent1.__name__ in str(exc_info.value)


def test_avoid_multilevel() -> None:
    """same conflict but on different level"""
    with pytest.raises(ConfigMultiInheritanceError) as exc_info:
        _ = ConfigChildMulti()
    assert "common" in str(exc_info.value)
    assert ConfigParent0.__name__ in str(exc_info.value)
    assert ConfigParent1.__name__ in str(exc_info.value)


def test_no_conflict_inheritance() -> None:
    """Test that inheritance will work as expected when there's no conflict"""
    conf = ConfigChildOk()
    assert isinstance(conf.unique_child, str)
    assert conf.unique_child == "child-ok"
    assert isinstance(conf.unique_0, int)
    assert conf.unique_0 == 0
    assert isinstance(conf.unique_2, int)
    assert conf.unique_2 == 2
