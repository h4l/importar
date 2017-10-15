import pytest
from unittest.mock import MagicMock

from importar import ImportOperation, ImportType, ImportOperationHandler


def mock_handler():
    return MagicMock(spec=ImportOperationHandler)


@pytest.mark.parametrize('record_type, import_type', [
    ('foo', ImportType.FULL_SYNC),
    ('foo', ImportType.PARTIAL_UPDATE),
    (object(), ImportType.FULL_SYNC),
    (object(), ImportType.PARTIAL_UPDATE)
])
def test_import_operation_has_record_and_import_types(
    record_type, import_type):

    iop = ImportOperation(record_type, import_type)

    assert iop.record_type is record_type
    assert iop.import_type is import_type


@pytest.fixture(scope='function')
def iop():
    return ImportOperation(object(), ImportType.FULL_SYNC)


def test_no_handlers_are_defined_initially(iop):
    assert len(iop.handlers) == 0


def test_handlers_can_be_attached(iop):
    handler = mock_handler()
    iop.attach_handler(handler)
    assert iop.handlers == [handler]


def test_multiple_handlers_can_be_attached(iop):
    handler1 = mock_handler()
    handler2 = mock_handler()
    iop.attach_handler(handler1)
    iop.attach_handler(handler2)
    assert iop.handlers == [handler1, handler2]


def test_handlers_can_be_detached(iop):
    handler1 = mock_handler()
    iop.attach_handler(handler1)
    iop.detach_handler(handler1)
    assert len(iop.handlers) == 0


def test_handlers_attached_more_than_once_are_recorded_once(iop):
    handler1 = mock_handler()
    iop.attach_handler(handler1)
    iop.attach_handler(handler1)
    assert len(iop.handlers) == 1


def test_detaching_unattached_handlers_has_no_effect(iop):
    iop.detach_handler(mock_handler())


def test_attached_handlers_must_be_iophandler_instances(iop):
    with pytest.raises(ValueError):
        iop.attach_handler(object())


def test_handlers_to_be_detached_neednt_be_iophandler_instances(iop):
    iop.detach_handler(object())


def test_repr():
    r = repr(ImportOperation('abcd', ImportType.FULL_SYNC))
    assert 'ImportOperation' in r
    assert 'abcd' in r
    assert 'ImportType.FULL_SYNC' in r
