from django.dispatch import Signal

from importar import import_started


def test_import_signal_exists():
    assert isinstance(import_started, Signal)
