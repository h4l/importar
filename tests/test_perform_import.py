import itertools
from unittest.mock import MagicMock, call
from contextlib import contextmanager

import pytest
from django.dispatch import Signal

from patronsdatasrc import (
    import_started, perform_import, ImportType, ImportOperation,
    ImportOperationHandler, ImportRecord, ID, ImportOperationError)


@contextmanager
def import_started_receiver(receiver):
    import_started.connect(receiver)
    yield receiver
    import_started.disconnect(receiver)


def test_import_started_event_is_fired():
    with import_started_receiver(MagicMock()) as mock_handler:
        perform_import('foo', ImportType.FULL_SYNC, [])

        assert mock_handler.called_once()


def test_import_started_receiver_receives_importop_as_sender():
    with import_started_receiver(MagicMock()) as mock_handler:
        perform_import('foo', ImportType.FULL_SYNC, [])

        _, kwargs = mock_handler.call_args
        assert isinstance(kwargs['sender'], ImportOperation)


@pytest.mark.parametrize('record_type, import_type', [
    ('abc', ImportType.FULL_SYNC),
    ('abc', ImportType.PARTIAL_UPDATE),
    (object(), ImportType.FULL_SYNC),
    (object(), ImportType.PARTIAL_UPDATE),
])
def test_importop_has_record_and_import_types(record_type, import_type):
    with import_started_receiver(MagicMock()) as mock_handler:
        perform_import(record_type, import_type, [])

        _, kwargs = mock_handler.call_args
        iop = kwargs['sender']

        assert iop.record_type is record_type
        assert iop.import_type is import_type


def test_records_must_be_importrecord_instances():
    with pytest.raises(ImportOperationError) as exc_info:
        perform_import('foo', ImportType.FULL_SYNC, [object()])

    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.fixture(scope='function')
def mock_iop_handler():
    handler = MagicMock(spec=ImportOperationHandler)

    iop = None
    def receiver(*args, sender=None, **kwargs):
        nonlocal iop
        iop = sender
        iop.attach_handler(handler)

    with import_started_receiver(receiver):
        yield handler
        iop.detach_handler(handler)


@pytest.mark.parametrize('records', [
    [ImportRecord([ID('foo', 'x-{}'.format(x))], x) for x in [1, 2, 3]],
    # Generators work too
    (ImportRecord([ID('foo', 'x-{}'.format(x))], x)
     for x in [object(), object(), object()])
])
def test_records_are_provided_to_handlers_registered_with_importop(
    mock_iop_handler, records):

    # tee in case it's a one shot generator
    in_records, expected_records = itertools.tee(records)
    iop = perform_import('foo', ImportType.FULL_SYNC, in_records)

    mock_iop_handler.on_record_available.assert_has_calls([
        call(iop, record) for record in expected_records])


def test_handlers_get_call_to_finished_func_after_records(mock_iop_handler):

    def assert_finished_not_called(iop, record):
        mock_iop_handler.on_import_finished.assert_not_called()


    mock_iop_handler.on_record_available.side_effect = assert_finished_not_called

    iop = perform_import('foo', ImportType.FULL_SYNC,
                         [ImportRecord([ID(1, 1)], 1)])

    assert mock_iop_handler.on_record_available.call_count == 1
    assert mock_iop_handler.on_import_finished.call_count == 1


def test_import_failed_not_called_on_successful_imports(mock_iop_handler):
    iop = perform_import('foo', ImportType.FULL_SYNC, [])

    assert mock_iop_handler.on_import_finished.call_count == 1
    assert mock_iop_handler.on_import_failed.call_count == 0


def test_import_failed_called_when_record_generator_raises(mock_iop_handler):

    def records():
        yield ImportRecord([ID(1, 1)], 1)
        raise ValueError('boom')

    with pytest.raises(ImportOperationError):
        perform_import('foo', ImportType.FULL_SYNC, records())

    assert mock_iop_handler.on_record_available.call_count == 1
    assert mock_iop_handler.on_import_finished.call_count == 0
    assert mock_iop_handler.on_import_failed.call_count == 1


def test_import_failed_called_when_record_is_invalid(mock_iop_handler):

    with pytest.raises(ImportOperationError):
        perform_import('foo', ImportType.FULL_SYNC, [object()])

    assert mock_iop_handler.on_record_available.call_count == 0
    assert mock_iop_handler.on_import_finished.call_count == 0
    assert mock_iop_handler.on_import_failed.call_count == 1


def test_import_failed_called_when_handler_raises(mock_iop_handler):

    mock_iop_handler.on_record_available.side_effect = ValueError('boom')

    with pytest.raises(ImportOperationError):
        perform_import('foo', ImportType.FULL_SYNC, [ImportRecord([ID(1, 1)], 1)])

    assert mock_iop_handler.on_record_available.call_count == 1
    assert mock_iop_handler.on_import_finished.call_count == 0
    assert mock_iop_handler.on_import_failed.call_count == 1
