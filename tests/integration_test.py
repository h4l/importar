'''
The test following demonstrates how this module can be used to listen for and
process incoming data.

The only significant difference due to this being a test is the need to
disconnect the receiver function from the global import_started signal. This is
to avoid interference with tests. The import_started_receiver context manager
is used to do this, but a normal app can just connect to the signal once.
'''
import pytest

from importar import (
    GeneratorImportOperationHandler, ImportOperation, ImportType,
    ImportOperationError, perform_import, ImportRecord, ID
)
from test_perform_import import import_started_receiver


def test_integration():
    # A mock "database" of records
    db = dict()

    # The generator which
    def record_handler(import_operation, records):
        to_update = dict()
        to_remove = set()

        for record in records:
            fid = get_f_id(record)

            if record.is_deleted():
                to_remove.add(fid)
            else:
                to_update[fid] = record.data

            # Yield is required to allow the next record to be made available
            # and allow other handlers to run. An exception is rail
            yield

        if import_operation.import_type is ImportType.FULL_SYNC:
            db.clear()
        else:
            for fid in to_remove:
                del db[fid]
        db.update(to_update)

    def import_listener(*args, sender=None, **kwargs):
        import_op = sender

        # We're handling the all-important foo records
        if import_op.record_type != 'foo':
            return

        handler = GeneratorImportOperationHandler(import_op, record_handler)
        import_op.attach_handler(handler)

    # Can use @receiver(import_started) decorator in normal app
    with import_started_receiver(import_listener):

        # Not a type we're interested in - these get ignored
        perform_import('bar', ImportType.FULL_SYNC, [
            ImportRecord([ID('x', 0)], object())
        ])

        assert len(db) == 0

        perform_import('foo', ImportType.FULL_SYNC, [
            ImportRecord([ID('f', 'a')], 'abc'),
            ImportRecord([ID('f', 'd')], 'def'),
        ])

        assert db == {
            'a': 'abc',
            'd': 'def'
        }

        # Update/delete just what's specified
        perform_import('foo', ImportType.PARTIAL_UPDATE, [
            # Delete this
            ImportRecord([ID('f', 'd')], None),
            # Add these
            ImportRecord([ID('f', 'g')], 'ghi'),
            ImportRecord([ID('f', 'j')], 'jkl'),
        ])

        assert db == {
            'a': 'abc',
            'g': 'ghi',
            'j': 'jkl'
        }

        # Replace everything
        perform_import('foo', ImportType.FULL_SYNC, [
            ImportRecord([ID('f', 'm')], 'mno'),
            ImportRecord([ID('f', 'p')], 'pqr'),
        ])

        assert db == {
            'm': 'mno',
            'p': 'pqr'
        }


def get_f_id(record):
    return next(i.value for i in record.ids if i.type == 'f')
