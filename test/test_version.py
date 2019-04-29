import pytest


def test_version():
    import hysds
    assert hysds.__version__ == "0.3.0"
