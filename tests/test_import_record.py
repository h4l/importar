import pytest

from patronsdatasrc import ImportRecord, ID


@pytest.fixture(scope='function')
def data_obj():
    return object()


@pytest.fixture(scope='function')
def ids():
    return [ID(1, 1)]


@pytest.fixture(scope='function')
def ir(ids, data_obj):
    return ImportRecord(ids, data_obj)


def test_no_data_indicates_deleted_record(ids):
    ir = ImportRecord(ids, None)

    assert ir.is_deleted() is True


def test_duplicate_ids_are_ignored(data_obj):
    ir = ImportRecord([
        ID(1, 2),
        ID(1, 2),
        ID(1, 3),
        ID(1, 3),
        ID(2, 3),
        ID(2, 3),
    ], data_obj)

    assert ir.ids == {ID(1, 2), ID(1, 3), ID(2, 3)}


def test_ids_must_be_id_instances():
    with pytest.raises(ValueError) as excinfo:
        ImportRecord(['abc'], None)

    assert 'not all ids were ID instances' in str(excinfo.value)


def test_has_data(ir, data_obj):
    assert ir.data is data_obj


def test_data_field_cant_be_modified(ir):
    with pytest.raises(AttributeError):
        ir.data = object()

def test_ids_field_cant_be_modified(ir):
    with pytest.raises(AttributeError):
        ir.ids = {ID(1, 2)}


def test_ids_cant_be_modified(ir):
    with pytest.raises(Exception):
        ir.ids.add(ID(2, 3))
