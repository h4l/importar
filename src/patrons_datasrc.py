from enum import Enum

from django.dispatch import Signal


import_started = Signal()
record_available = Signal(providing_args=['record'])
import_failed = Signal()
import_finished = Signal()


ImportType = Enum('ImportType', 'FULL_SYNC PARTIAL_UPDATE')


class ImportError(RuntimeError):
    pass


def _import_failed(operation, err):
    # Always notify everyone of the operation failing
    import_failed.send_robust(operation)

    raise ImportError('Failed to send import_started: '
                          'receiver raised exception') from err

def _first_error(responses):
    '''
    Check a Signal.send() result list for failures.

    Returns: The first response value which is an Exception instance or None
    '''
    return next((resp for (_, resp) in responses
                 if isinstance(resp, Exception)), None)

# TODO: can we use x = yield coroutines instead of all these signals?
#   seems yes, but we should provide a generator which makes a generator
#   implement some sane interface. e.g. you have to send(None) before sending a
#   real value to an actual generator, which is an odd detail which shouldn't
#   be exposed.

def perform_import(operation, records):
    # Contract: everyone who receives an import_started signal MUST receive
    # either an import_failed or import_finished signal. This is guaranteed
    # through the use of send_robust() to send the starting and ending signals.
    err = _first_error(import_starting.send_robust(operation))
    if err is not None:
        _import_failed('Failed to send import_started: '
                       'receiver raised exception', err)

    # Iterate manually to more easily determine if an error was from a receiver
    # or the record generator
    records_iter = iter(records)

    while True:
        try:
            record = next(records_iter)
        except StopIteration:
            break
        except Exception as err:
            _import_failed('Record generator raised exception', err)

        try:
            record_available.send(operation, record=record)
        except Exception as err:
            _import_failed()

    try:
        for record in records:
            record_available.send(operation, record=record)
    except Exception as err:
        raise ImportError('Failed to send import_started: '
                          'receiver raised exception') from err





class ImportOperation(object):
    """
    Identifies a unique, ongoing import operation, which results in zero or more
    user records being generated.
    """
    def __init__(self, record_type, import_type):
        self._record_type = record_type
        self._import_type = import_type

        self._handlers = []

    @property
    def record_type(self):
        return self._record_type

    @property
    def import_type(self):
        return self._import_type

    def attach_handler(self, handler):
        if not handler in self._handlers:
            self._handlers.append(handler)

    def detach_handler(self, handler):
        try:
            self._handlers.remove(handler)
        except ValueError:
            pass

    def __repr__(self):
        return 'ImportOperation({!r}, {!r})'.format(
            self.record_type, self.import_type)


class ImportRecord(object):
    def __init__(self, ids, data):
        self._ids = dict(ids)
        self._data = data

    def get_ids(self):
        return self._ids

    def is_deleted(self):
        return self._data is None

    def get_data(self):
        return self._data

    def __repr__(self):
        return 'ImportRecord({!r}, {!r})'.format(
            self.get_ids(), self.get_data())


class ImportOperationHandler(object):

    def on_record_available(self, operation, record):
        pass

    def on_import_failed(self, operation):
        pass

    def on_import_finished(self, operation):
        pass

