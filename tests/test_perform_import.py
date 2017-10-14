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


@contextmanager
def handlers_attached(*handlers):
    iop = None
    def receiver(*args, sender=None, **kwargs):
        nonlocal iop
        assert iop is None
        iop = sender

        for handler in handlers:
            iop.attach_handler(handler)

    with import_started_receiver(receiver):
        yield handlers

        assert iop is not None, 'import_started signal not received'

        for handler in handlers:
            iop.detach_handler(handlers)


@pytest.fixture(scope='function')
def mock_iop_handler():
    handler = MagicMock(spec=ImportOperationHandler)

    with handlers_attached(handler):
        yield handler


def test_import_started_event_is_fired():
    with import_started_receiver(MagicMock()) as mock_handler:
        perform_import('foo', ImportType.FULL_SYNC, [])

        assert mock_handler.called_once()


def test_import_started_receiver_receives_importop_as_sender():
    with import_started_receiver(MagicMock()) as mock_handler:
        iop = perform_import('foo', ImportType.FULL_SYNC, [])

        _, kwargs = mock_handler.call_args
        assert kwargs['sender'] is iop


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


@pytest.mark.parametrize('import_type', [
    'abc', object(), ImportType, None
])
def test_import_type_must_be_importtype_instances(import_type):
    with pytest.raises(ValueError) as excinfo:
        perform_import('foo', import_type, [])

    assert 'import_type was not an ImportType' in str(excinfo.value)


def test_records_must_be_importrecord_instances():
    with pytest.raises(ImportOperationError) as exc_info:
        perform_import('foo', ImportType.FULL_SYNC, [object()])

    assert isinstance(exc_info.value.__cause__, ValueError)


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
        perform_import('foo', ImportType.FULL_SYNC, [
            ImportRecord([ID(1, 1)], 1)
        ])

    assert mock_iop_handler.on_record_available.call_count == 1
    assert mock_iop_handler.on_import_finished.call_count == 0
    assert mock_iop_handler.on_import_failed.call_count == 1


def test_import_failed_called_when_import_started_receiver_raises(
    mock_iop_handler):
    '''
    Registered handlers get an import failed event when an import_started
    receiver fails.
    '''

    # Register another import_started receiver which will fail
    def failing_receiver(*args, **kwargs):
        raise ValueError('boom')

    with import_started_receiver(failing_receiver):
        with pytest.raises(ImportOperationError) as excinfo:
            perform_import('foo', ImportType.FULL_SYNC, [])

        assert ('import_started signal receiver raised exception' in str(excinfo.value))

    assert mock_iop_handler.on_import_failed.call_count == 1


def test_all_failed_handlers_called_despite_one_failing():
    '''
    When invoking failed handlers, all get invoked, even if a preceding
    failed handler itself raises an error.
    '''

    handler_a = MagicMock(spec=ImportOperationHandler)
    handler_b = MagicMock(spec=ImportOperationHandler)

    def handle_failed_a(operation):
        # b is called after we are
        handler_b.on_import_failed.assert_not_called()
        # Our handler fails for some reason...
        raise ValueError('boom')

    def handle_failed_b(operation):
        # a has been called before us (and failed)
        assert handler_a.on_import_failed.call_count == 1

    handler_a.on_import_failed.side_effect = handle_failed_a
    handler_b.on_import_failed.side_effect = handle_failed_b

    with handlers_attached(handler_a, handler_b):
        with pytest.raises(ImportOperationError) as excinfo:
            perform_import('foo', ImportType.FULL_SYNC,
                # Fails due to division by zero
                (1/0 for _ in [1]))

        assert str(excinfo.value) == 'record generator raised exception'

    assert handler_a.on_import_failed.call_count == 1
    assert handler_b.on_import_failed.call_count == 1


def test_all_finished_handlers_called_despite_one_failing():
    handler_a = MagicMock(spec=ImportOperationHandler)
    handler_b = MagicMock(spec=ImportOperationHandler)

    def handle_finished_a(operation):
        # b is called after we are
        handler_b.on_import_finished.assert_not_called()
        # Our handler fails for some reason...
        raise ValueError('boom')

    def handle_finished_b(operation):
        # a has been called before us (and failed)
        assert handler_a.on_import_finished.call_count == 1

    handler_a.on_import_finished.side_effect = handle_finished_a
    handler_b.on_import_finished.side_effect = handle_finished_b

    with handlers_attached(handler_a, handler_b):
        with pytest.raises(ImportOperationError) as excinfo:
            perform_import('foo', ImportType.FULL_SYNC, [])

        assert str(excinfo.value) == 'handler raised from on_import_finished()'

    assert handler_a.on_import_finished.call_count == 1
    assert handler_b.on_import_finished.call_count == 1
