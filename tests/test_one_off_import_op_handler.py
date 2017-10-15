import pytest

from importar import (
    OneOffImportOperationHandler, ImportOperationHandler, ImportType,
    ImportRecord, ID, ImportOperation
)


@pytest.fixture(scope='function')
def iop_a():
    return ImportOperation('foo', ImportType.FULL_SYNC)


@pytest.fixture(scope='function')
def iop_b():
    return ImportOperation('foo', ImportType.FULL_SYNC)


@pytest.fixture(scope='function')
def oneoff_a(iop_a):
    return OneOffImportOperationHandler(iop_a)


@pytest.fixture(scope='function')
def record():
    return ImportRecord([ID(1, 1)], object())


def test_iop_a_and_b_have_same_fields(iop_a, iop_b):
    assert iop_a.record_type is iop_b.record_type
    assert iop_a.import_type is iop_b.import_type
    assert iop_a is not iop_b
    assert iop_a != iop_b


def test_is_instance_of_import_op_handler(oneoff_a):
    assert isinstance(oneoff_a, ImportOperationHandler)


def test_handler_methods_allow_own_operation(oneoff_a, iop_a, record):
    oneoff_a.on_record_available(iop_a, record)
    oneoff_a.on_import_failed(iop_a)
    oneoff_a.on_import_finished(iop_a)


def test_handler_methods_reject_operations_other_than_own(oneoff_a, iop_b):
    with pytest.raises(ValueError):
        oneoff_a.on_record_available(iop_a, record)

    with pytest.raises(ValueError):
        oneoff_a.on_import_failed(iop_a)

    with pytest.raises(ValueError):
        oneoff_a.on_import_finished(iop_a)
