from pynenc import Pynenc
from pynenc.conf import config_base


class ConfTest(config_base.ConfigBase):
    cf = config_base.ConfigField(0)


def test_isolation_both_instantiate_both_first() -> None:
    """test that one instance do not affect the other when both are instantiated first"""
    conf1 = ConfTest()
    conf2 = ConfTest()
    conf1.cf = 1
    assert conf1.cf == 1
    assert conf2.cf == 0


def test_isolation_both_instantiate_diff() -> None:
    """test that one instance do not affect the other when both are instantiated second"""
    conf1 = ConfTest()
    conf1.cf = 1
    assert conf1.cf == 1
    conf2 = ConfTest()
    assert conf2.cf == 0


def test_with_pynenc_apps_instantiating_both_first() -> None:
    """test with configs of 2 pynenc apps when both are instantiated first"""
    app1 = Pynenc()
    app2 = Pynenc()
    assert app1.orchestrator.conf.cycle_control is True
    assert app2.orchestrator.conf.cycle_control is True
    app1.orchestrator.conf.cycle_control = False
    assert app1.orchestrator.conf.cycle_control is False
    assert app2.orchestrator.conf.cycle_control is True


def test_with_pynenc_apps_instantiating_diff() -> None:
    """test with configs of 2 pynenc apps when both are instantiated second"""
    app1 = Pynenc()
    app1.orchestrator.conf.cycle_control = False
    app2 = Pynenc()
    assert app1.orchestrator.conf.cycle_control is False
    assert app2.orchestrator.conf.cycle_control is True
