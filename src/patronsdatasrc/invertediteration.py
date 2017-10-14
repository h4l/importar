'''
Allows values to be pushed into a loop by a supplier, rather than pulled
from a supplier by a loop.

This is best explained by an example:

    >>> def consumer(things):
    ...     for t in things:
    ...         print('got a thing:', repr(t))
    ...         yield
    ...     print('consumed all the things!')
    >>> controller = InvertedIterationController(consumer)
    >>> controller.transmit_value('abc')
    got a thing: 'abc'
    >>> controller.transmit_value('def')
    got a thing: 'def'
    >>> controller.end()
    consumed all the things!
'''

import enum

__all__ = ['InvertedIterationController']

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

        assert self.state == State.AT_END
        raise StopIteration()

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


class InvertedIterationController:
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
        self.iteration_state = IterationState()
        self.consumer = bind_consumer(self._feeder())
        self.step_count = 0
        self.yield_count = 0

    def _feeder(self):
        while True:
            self.yield_count += 1
            self._validate_in_sync()

            yield next(self.iteration_state)

    def _validate_in_sync(self):
        if self.yield_count > self.step_count:
                raise InvertedIterationError(
                    'Consumer attempted to access a second value without '
                    'yielding. (Did you forget a yield statement in your for '
                    'loop?)')

        elif self.yield_count < self.step_count:
            raise InvertedIterationError(
                'Consumer didn\'t advance its iterator. (Are you forget '
                'to run a for loop over the iterator?)')

    def _step(self):
        assert self.yield_count == self.step_count
        try:
            self.step_count += 1

            # Have the consumer pull the next value from our _feeder() generator
            next(self.consumer)

            # The consumer must advance our _feeder() when we advance it. If
            # it doesn't the counts will be out of sync.
            self._validate_in_sync()
        except InvertedIterationError:
            raise
        except StopIteration:
            if not self.iteration_state.is_at_end:
                raise InvertedIterationError(
                    'Consumer unexpectedly raised StopIteration. (Perhaps '
                    'you\'re breaking before consuming the whole iterator.)')
            self._validate_in_sync()
        except Exception:
            self._validate_in_sync()
            raise


    def transmit_value(self, value):
        self.iteration_state.register_next_val(value)
        self._step()

    def fail(self, err):
        self.iteration_state.register_error(err)
        self._step()

    def end(self):
        self.iteration_state.register_end()
        self._step()

    @property
    def is_active(self):
        return not self.iteration_state.is_at_end
