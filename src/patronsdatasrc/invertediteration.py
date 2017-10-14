'''
Allows values to be pushed into a loop by a supplier, rather than pulled
from a supplier by a loop.

This is best explained by an example:

    >>> def consumer(things):
    ...     for t in things:
    ...         print('got a thing:', repr(t))
    ...         yield
    ...     print('consumed all the things!')
    >>> controller = InvertedIteartionController(consumer)
    >>> controller.transmit_value('abc')
    got a thing: 'abc'
    >>> controller.transmit_value('def')
    got a thing: 'def'
    >>> controller.end()
    consumed all the things!
'''

import enum

__all__ = ['InversionIterator']

State = enum.Enum('State', 'AWAITING_VAL HAS_VAL AT_ERR AT_END')


class IterationState:
    '''
    Represents the state required to drive one cycle (__next__()) call of an
    iterator. This allows for the decoupling of a party providing iteration
    values and a party consuming them.
    '''
    def __init__(self):
        self.state = State.AWAITING_VAL
        self.val = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.state == State.AWAITING_VAL:
            raise RuntimeError('__next__ called before next val was provided')
        elif self.state == State.HAS_VAL:
            val = self.val
            self.val = None
            self.state = State.AWAITING_VAL
            return val
        elif self.state == State.AT_ERR:
            err = self.val
            self.val = None
            self.state = State.AT_END
            raise err
        elif self.state == State.AT_END:
            raise StopIteration()
        raise AssertionError('Unknown state', self.state)

    def register_next_val(self, val):
        if self.state != State.AWAITING_VAL:
            raise RuntimeError('register_next_value() called in unexpected state: {}'
                               .format(self.state))

        self.val = val
        self.state = State.HAS_VAL

    def register_error(self, err):
        if self.state != State.AWAITING_VAL:
            raise RuntimeError('register_error() called in unexpected state: {}'
                               .format(self.state))
        self.val = err
        self.state = State.AT_ERR

    def register_end(self):
        if self.state != State.AWAITING_VAL:
            raise RuntimeError('register_end() called in unexpected state: {}'
                               .format(self.state))

        self.state = State.AT_END

    @property
    def is_at_end(self):
        return self.state == State.AT_END


class InvertedIterationError(Exception):
    pass


class InvertedIteartionController:
    '''
    Allows for a for loop to consume an asynchronously produced series of
    values, as if the for loop was in control of, and blocking while each value
    is made available. Control is inverted, so that the for loop is driven by
    the iterable being iterated rather than the iterable being driven by the
    consuming loop.

    This allows for a simple for loop to run over an asynchronously produced
    series of values, and for a context manager to control the lifetime of
    resources interacting with asynchronously produced values.

    A consumer is expected to be a generator which iterates over the
    controller's feeder iterator; yielding in each iteration to secede control
    to allow the next value to be produced.
    '''

    def __init__(self, bind_consumer):
        '''
        Create a Controller of a consumer, responsible for driving the
        consumer's iteration.

        Args:
            bind_consumer: The function
        '''
        self.inversion_state = InversionState()
        self.consumer = bind_consumer(self._feeder())
        self.step_count = 0
        self.yield_count = 0

    def _feeder(self):
        while True:
            if self.yield_count > self.step_count:
                raise InvertedIterationError(
                    'Consumer attempted to access a second value without '
                    'yielding. (Did you forget a yield statement in your for '
                    'loop?)')
            assert self.yield_count == self.step_count

            self.yield_count += 1
            yield next(self.inversion_state)

    def _step(self):
        try:
            assert self.yield_count == self.step_count
            # Have the consumer pull the next value from our _feeder() generator
            next(consumer)
            self.step_count += 1

            if self.yield_count < self.step_count:
                raise InvertedIterationError(
                    'Consumer didn\'t advance its iterator. (Are you forget '
                    'to run a for loop over the iterator?)')
            assert self.yield_count == self.step_count
        except StopIteration:
            if not self.inversion_state.is_at_end:
                raise RuntimeError(
                    'consumer unexpectedly raised StopIteration', consumer)

    def transmit_value(self, value):
        self.inversion_state.register_next_val(value)
        self._step()

    def fail(self, err):
        self.inversion_state.register_error(err)
        self._step()

    def end(self):
        self.inversion_state.register_end()
        self._step()
