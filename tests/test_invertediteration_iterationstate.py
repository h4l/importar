import pytest

from importar.invertediteration import (
    State, IterationState
)


def test_four_states_exist():
    assert len(State) == 4
    State.AWAITING_VAL
    State.HAS_VAL
    State.AT_ERR
    State.AT_END


@pytest.fixture(scope='function')
def iterstate():
    return IterationState()


@pytest.fixture(scope='function')
def val():
    return object()


@pytest.fixture(scope='function')
def iterstate_val(iterstate, val):
    iterstate.register_next_val(val)
    return iterstate


@pytest.fixture(scope='function')
def iterstate_end(iterstate):
    iterstate.register_end()
    return iterstate


@pytest.fixture(scope='function')
def err():
    return ValueError('foo')


@pytest.fixture(scope='function')
def iterstate_err(iterstate, err):
    iterstate.register_error(err)
    return iterstate


def test_initial_state_is_awaiting(iterstate):
    assert iterstate.state is State.AWAITING_VAL


def test_can_be_ended_while_awaiting(iterstate):
    iterstate.register_end()
    assert iterstate.state is State.AT_END
    assert iterstate.is_at_end


def test_cant_request_value_while_awaiting(iterstate):
    with pytest.raises(RuntimeError) as excinfo:
        next(iterstate)

    assert '__next__ called before next val was provided' in str(excinfo.value)


def test_requesting_value_at_end_results_in_stopiteration(iterstate_end):
    with pytest.raises(StopIteration):
        next(iterstate_end)


def test_error_can_be_registered_while_awaiting(iterstate):
    iterstate.register_error(ValueError('foo'))
    assert iterstate.state is State.AT_ERR


def test_requesting_value_raises_error_after_error_registered(iterstate):
    err = ValueError('foo')
    iterstate.register_error(err)

    with pytest.raises(ValueError) as excinfo:
        next(iterstate)

    assert excinfo.value is err


def test_is_at_end_after_error_is_consumed(iterstate):
    iterstate.register_error(ValueError('foo'))

    with pytest.raises(ValueError):
        next(iterstate)

    assert iterstate.is_at_end


def test_value_can_be_registered_while_awating(iterstate):
    iterstate.register_next_val(object())


def test_state_is_has_val_after_value_registered(iterstate_val):
    assert iterstate_val.state is State.HAS_VAL


def test_next_produces_registered_val(iterstate_val, val):
    assert next(iterstate_val) is val


def test_cant_register_second_value_while_has_value(iterstate_val):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_val.register_next_val(object())

    assert ('register_next_value() called in unexpected state'
            in str(excinfo.value))


def test_cant_provide_val_at_end(iterstate_end):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_end.register_next_val(object())

    assert ('register_next_value() called in unexpected state'
            in str(excinfo.value))


def test_cant_provide_val_at_error(iterstate_err):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_err.register_next_val(object())

    assert ('register_next_value() called in unexpected state'
            in str(excinfo.value))


def test_cant_register_end_at_error(iterstate_err):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_err.register_end()

    assert ('register_end() called in unexpected state'
            in str(excinfo.value))


def test_cant_register_error_while_has_value(iterstate_val):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_val.register_error(ValueError('foo'))

    assert ('register_error() called in unexpected state'
            in str(excinfo.value))


def test_cant_register_end_while_has_value(iterstate_val):
    with pytest.raises(RuntimeError) as excinfo:
        iterstate_val.register_end()

    assert ('register_end() called in unexpected state'
            in str(excinfo.value))
