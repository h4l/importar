from unittest.mock import MagicMock, call
from collections.abc import Iterable

import pytest

from patronsdatasrc import (
    GeneratorImportOperationHandler, ImportOperation, ImportType,
    ImportOperationError
)


@pytest.fixture(scope='function')
def handler():
    return MagicMock()


@pytest.fixture(scope='function')
def consumer(handler):
    def consumer(operation, records):
        handler.start(operation)
        try:
            for record in records:
                handler.handle(record)
                yield
            handler.end(operation)
        except Exception as e:
            handler.exception(e)

    mock = MagicMock()
    mock.side_effect = consumer
    return mock


@pytest.fixture(scope='function')
def iop():
    return ImportOperation('foo', ImportType.FULL_SYNC)


@pytest.fixture(scope='function')
def gen_handler(iop, consumer):
    return GeneratorImportOperationHandler(iop, consumer)


def test_consumer_invoked_with_iop_and_iterable(gen_handler, consumer, iop):
    assert consumer.call_count == 1
    (arg_iop, arg_iter), _ = consumer.call_args

    assert arg_iop is iop
    assert isinstance(arg_iter, Iterable)


def test_new_consumer_started_after_first_record(gen_handler, iop, handler):
    assert handler.start.call_count == 0
    gen_handler.on_record_available(iop, object())
    assert handler.start.call_count == 1


def test_new_consumer_started_when_import_finished(gen_handler, iop, handler):
    assert handler.start.call_count == 0
    gen_handler.on_import_finished(iop)
    assert handler.start.call_count == 1


def test_new_consumer_started_when_import_finished(gen_handler, iop, handler):
    assert handler.start.call_count == 0
    gen_handler.on_import_failed(iop)
    assert handler.start.call_count == 1


def test_import_failed_reported_as_importoperationerror(
    gen_handler, iop, handler):

    gen_handler.on_import_failed(iop)

    (err,), _ = handler.exception.call_args
    assert isinstance(err, ImportOperationError)


def test_available_records_are_consumed(gen_handler, iop, handler):
    gen_handler.on_record_available(iop, 'abc')
    gen_handler.on_record_available(iop, 'def')
    gen_handler.on_import_finished(iop)

    handler.start.assert_called_with(iop)
    handler.handle.assert_has_calls([
        call('abc'),
        call('def')
    ])
    assert handler.end.call_count == 1


def test_handler_which_failed_on_record_available_doesnt_fail_controller_when_controller_is_dead(
    gen_handler, iop, handler):
    # If the handler's controller fails when handling a record, the failure
    # will result in each handler getting a failed event. Because the
    # controller is dead (its consumer generator has failed) it's not possible
    # to notify it of the failure. Not that that matters, as the consumer will
    # (or can) know that it itself failed through its own try: except:
    # blocks/context managers, etc.

    # Have the consumer die
    handler.start.side_effect = ValueError('boom')

    with pytest.raises(ValueError) as excinfo:
        gen_handler.on_record_available(iop, 'abc')

    assert str(excinfo.value) == 'boom'
    handler.handle.assert_not_called()

    # Does nothing as the controller/consumer are dead
    gen_handler.on_import_failed(iop)

    handler.exception.assert_not_called()


def test_handler_which_failed_on_import_finished_doesnt_fail_controller_when_controller_is_dead(
    gen_handler, iop, handler):
    # Note that currently failed is never triggered after finished is, so this
    # interaction (calling import_failed after import_finished) never happens.
    # e.g. if handlers fail while handling finished, other handlers are not
    # notified.

    # Have the consumer die
    handler.start.side_effect = ValueError('boom')

    with pytest.raises(ValueError) as excinfo:
        gen_handler.on_import_finished(iop)

    assert str(excinfo.value) == 'boom'
    handler.end.assert_not_called()

    # Does nothing as the controller/consumer are dead.
    # Would raise if controller.fail() was called as a result of this call.
    gen_handler.on_import_failed(iop)
