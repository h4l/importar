from unittest.mock import MagicMock, call
from collections.abc import Iterable

import pytest

from patronsdatasrc.invertediteration import (
    InvertedIterationController, InvertedIterationError
)


def test_creating_controller_creates_consumer_from_bind_func():
    bind_consumer = MagicMock()
    bind_consumer.return_value = object()

    controller = InvertedIterationController(bind_consumer)

    assert bind_consumer.call_count == 1
    args, _ = bind_consumer.call_args
    assert isinstance(args[0], Iterable)
    assert controller.consumer is bind_consumer.return_value


@pytest.fixture(scope='function')
def handler():
    return MagicMock()


@pytest.fixture(scope='function')
def consumer(handler):
    def consumer(feeder):
        try:
            for item in feeder:
                handler.handle(item)
                yield
            handler.end()
        except Exception as e:
            handler.exception(e)
    return consumer


@pytest.fixture(scope='function')
def controller(consumer):
    return InvertedIterationController(consumer)


def test_consumer_receives_transmitted_values(controller, handler):
    controller.transmit_value('a')
    controller.transmit_value('b')
    controller.transmit_value('c')

    assert handler.handle.has_calls([
        call('a'),
        call('b'),
        call('c')
    ])


def test_consumer_receives_end(controller, handler):
    controller.end()

    assert handler.end.call_count == 1


def test_is_active_initially(controller):
    assert controller.is_active


def test_is_not_active_after_end(controller):
    controller.end()
    assert not controller.is_active


def test_is_not_active_after_fail(controller):
    controller.fail(ValueError('foo'))
    assert not controller.is_active


def test_consumer_receives_error(controller, handler):
    err = ValueError('foo')
    controller.fail(err)

    assert handler.exception.called_with(err)


@pytest.fixture(scope='function')
def non_consuming_consumer():
    def non_consuming_consumer(feeder):
        # not pulling from feeder
        while True:
            yield

    return non_consuming_consumer


@pytest.fixture(scope='function')
def controller_with_non_consuming_consumer(non_consuming_consumer):
    return InvertedIterationController(non_consuming_consumer)


def test_transmitting_value_to_non_consuming_consumer_results_in_error(
    controller_with_non_consuming_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_non_consuming_consumer.transmit_value(object())

    assert 'Consumer didn\'t advance its iterator.' in str(excinfo.value)


def test_transmitting_failure_to_non_consuming_consumer_results_in_error(
    controller_with_non_consuming_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_non_consuming_consumer.fail(ValueError('foo'))

    assert 'Consumer didn\'t advance its iterator.' in str(excinfo.value)


def test_transmitting_end_to_non_consuming_consumer_results_in_error(
    controller_with_non_consuming_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_non_consuming_consumer.end()

    assert 'Consumer didn\'t advance its iterator.' in str(excinfo.value)


@pytest.fixture(scope='function')
def greedy_consumer():
    def greedy_consumer(feeder):
        # Don't yield, keep pulling from feeder
        for thing in feeder:
            if False:
                yield

    return greedy_consumer

@pytest.fixture(scope='function')
def controller_with_greedy_consumer(greedy_consumer):
    return InvertedIterationController(greedy_consumer)


def test_transmitting_value_to_non_yielding_consumer_results_in_error(
    controller_with_greedy_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_greedy_consumer.transmit_value(object())

    assert ('Consumer attempted to access a second value without yielding.'
            in str(excinfo.value))


# failure and end work on greedy consumers as they both kill
# the feeder itself, so it doesn't matter that they don't
# yield as they never get a chance to.
def test_transmitting_failure_to_non_yielding_consumer_works(
    controller_with_greedy_consumer):

    with pytest.raises(ValueError) as excinfo:
        controller_with_greedy_consumer.fail(ValueError('foo'))

    assert 'foo' == str(excinfo.value)


def test_transmitting_end_to_non_yielding_consumer_works(
    controller_with_greedy_consumer):

    controller_with_greedy_consumer.end()


@pytest.fixture(scope='function')
def lazy_consumer():
    def lazy_consumer(feeder):
        # Stops without consuming feeder
        if False:
            yield

    return lazy_consumer


@pytest.fixture(scope='function')
def controller_with_lazy_consumer(lazy_consumer):
    return InvertedIterationController(lazy_consumer)


def test_transmitting_value_to_lazy_consumer_results_in_error(
    controller_with_lazy_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_lazy_consumer.transmit_value(object())

    assert ('Consumer unexpectedly raised StopIteration.'
            in str(excinfo.value))


def test_transmitting_end_to_lazy_consumer_results_in_error(
    controller_with_lazy_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_lazy_consumer.end()

    # We expect the consumer to raise a StopIteration (which it does) so we
    # notice that it's not pulled from the feeder.
    assert ('Consumer didn\'t advance its iterator.'
            in str(excinfo.value))


def test_transmitting_failure_to_lazy_consumer_results_in_error(
    controller_with_lazy_consumer):

    with pytest.raises(InvertedIterationError) as excinfo:
        controller_with_lazy_consumer.fail(ValueError('foo'))

    assert ('Consumer unexpectedly raised StopIteration.'
            in str(excinfo.value))
