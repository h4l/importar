import abc
import collections
from enum import Enum
import logging
from functools import wraps

from django.dispatch import Signal


logger = logging.getLogger(__name__)

import_started = Signal()


ImportType = Enum('ImportType', 'FULL_SYNC PARTIAL_UPDATE')


class ImportOperationError(RuntimeError):
    pass


def _notify_handler_robust(operation, method):
    errors = []
    for h in operation.handlers:
        assert _hasmethod(h, method)
        try:
            getattr(h, method)(operation)
        except Exception as err:
            errors.append((h, err))
            logger.exception('Exception raised from handler\'s '
                             '{}() method. handler: {}'
                             .format(method, handler))
    return errors


def _notify_import_failed(operation):
    return _notify_handler_robust(operation, 'on_import_failed')


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

def perform_import(record_type, import_type, records):
    if not isinstance(import_type, ImportType):
        raise ValueError(
            'import_type was not an ImportType: {}'.format(import_type))

    operation = ImportOperation(record_type, import_type)

    # Contract: everyone who receives an import_started signal MUST receive
    # either an import_failed or import_finished signal. This is guaranteed
    # through the use of send_robust() to send the starting and ending signals.
    err = _first_error(import_started.send_robust(operation))
    if err is not None:
        _notify_import_failed(operation)
        raise ImportOperationError(
            'import_started signal receiver raised exception. receiver: {}'
            .format()) from err

    # Iterate manually to more easily determine if an error was from a receiver
    # or the record generator
    records_iter = iter(records)

    while True:
        try:
            record = next(records_iter)
            if not isinstance(record, ImportRecord):
                raise ValueError('record is not an ImportRecord instance: {}'
                                 .format(record), record)
        except StopIteration:
            break
        except Exception as err:
            _notify_import_failed(operation)
            raise ImportOperationError(
                'Record generator raised exception') from err

        try:
            for h in operation.handlers:
                h.on_record_available(operation, record)
        except Exception as err:
            _notify_import_failed(operation)
            raise ImportOperationError(
                'handler raised from on_record_available()') from err

    errors = _notify_handler_robust(operation, 'on_import_finished')
    if(errors):
        raise ImportOperationError('handler raised from on_import_finished()')

    return operation


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

    @property
    def handlers(self):
        return self._handlers

    def attach_handler(self, handler):
        _validate_import_handler(handler)
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


class ID(collections.namedtuple('ID', 'type value'.split()),
         metaclass=abc.ABCMeta):
    __slots__ = ()


class ImportRecord(collections.namedtuple('ImportRecord', ['ids', 'data']),
                   metaclass=abc.ABCMeta):
    __slots__ = ()

    def __new__(cls, ids, data):
        ids = frozenset(ids)
        if not all(isinstance(i, ID) for i in ids):
            raise ValueError('not all ids were ID instances: {}'.format(ids))

        return super(cls, ImportRecord).__new__(cls, ids, data)

    @property
    @abc.abstractmethod
    def idmap(self):
        return dict((id.type, id) for id in ids)

    @abc.abstractmethod
    def is_deleted(self):
        return self.data is None

    def __repr__(self):
        return 'ImportRecord({!r}, {!r})'.format(
            self.ids, self.data)


def _hasmethod(obj, name):
    return callable(getattr(obj, name, None))


def _validate_import_handler(handler):
    if not isinstance(handler, ImportOperationHandler):
        raise ValueError(
            'handler isn\'t a ImportOperationHandler. Use '
            'ImportOperationHandler.register(cls) if not subclassing. '
            'handler: {}'.format(handler))


class ImportOperationHandler(abc.ABC):
    '''
    An event handler for events triggered during a user data import operation.

    This is an Abstract Base Class and need not (but can) be subclassed.
    '''

    @abc.abstractmethod
    def on_record_available(self, operation, record):
        pass

    @abc.abstractmethod
    def on_import_failed(self, operation):
        pass

    @abc.abstractmethod
    def on_import_finished(self, operation):
        pass


class OneOffImportOperationHandler(ImportOperationHandler):
    '''
    A ImportOperationHandler which can only receive events for a single
    operation. Event handler methods throw ValueError if called with an
    operation other than that provided to the constructor.
    '''
    def __init__(self, operation, generator):
        self._operation = operation

    def _validate_operation_is_ours(self, operation):
        if self._operation is not operation:
            raise ValueError('received an import event from an operation '
                             'other than our own')

    def on_record_available(self, operation, record):
        super().on_record_available(operation, record)
        self._validate_operation_is_ours(operation)

    def on_import_failed(self, operation):
        super().on_import_failed(operation)
        self._validate_operation_is_ours(operation)

    def on_import_finished(self, operation):
        super().on_import_finished(operation)
        self._validate_operation_is_ours(operation)


class GeneratorImportOperationHandler(OneOffImportOperationHandler):
    #
    # Terminology:
    # handling generator: The generator provided by external code which does
    #           something with each record we discover.
    # driving generator: The generator which is iterated over by the handling
    #           generator to receive pending records
    #           pending record: An object which yields a record when awaited
    #           using `yield from`
    #

    def __init__(self, operation, handling_generator_creator):
        super().__init__(operation)

        self._driving_generator = self._driver()

        self._handling_generator = handling_generator_creator(
            operation, _driving_generator)

        self._pending_record = None

        def future_record():
            while self._next_record is not None:


                yield self._next_record

        self._next_record_provider

    @staticmethod
    def future(token, value):
        yield token
        return value

    def _driver(self):
        last_token = None
        while self._pending_record != None:
            token, value = self._pending_record
            if last_token is token:
                raise AssertionError(
                    'import operation handler generator attempted to obtain a '
                    'new record without yielding')

            yield self.future(token, value)


    def _ensure_generator_primed(self):
        if not self._generator_primed:
            next(self.generator)
            self._generator_primed = True

    def on_record_available(self, operation, record):
        super().on_record_available(operation, record)

        # If we've not yet seen a record we need to wait until we know the next
        # event before passing the record to the handling generator. This is
        # because we need to know if the generator feeding the handling
        if self._pending_record is None:
            self._pending_record = (object(), record)


        #self._ensure_generator_primed()
        #self._generator.send(record)

    def on_import_failed(self, operation):
        super().on_import_failed(operation)

        self._ensure_generator_primed()
        self._generator.throw(ImportOperationError, 'got on_import_failed()')


    def on_import_finished(self, operation):
        super().on_import_finished(operation)

        self._ensure_generator_primed()





def import_operation_handler(f):
    '''
    Implement an ImportOperationHandler using a generator function (which acts
    as a coroutine).

    f is a generator function which receives import records by yielding.

    @import_operation_handler
    def handler(import_operation, future_records):
        # set up
        tx = start_db_transaction()

        for future_record in future_records:
            record = yield from future_record

            save_record_to_db(record, db)

        # clean up
        tx.commit()
    '''
    @wraps(f)
    def wrapper(operation):
        pass # FIXME
    return wrapper


def import_operation_handler2(f):
    '''

    @import_operation_handler2
    def handler(operation, records)
        # set up
        tx = start_db_transaction()

        try:
            for r in records:
                save_record_to_db(record, tx)
        except:
            tx.roll_back()

        # clean up
        tx.commit()
    '''
    # @wraps(f)
    # def import_op_handler
