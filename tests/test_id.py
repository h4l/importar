from importar import ID


def test_new_id_types_can_be_registered():
    class MyID:
        pass

    ID.register(MyID)

    assert isinstance(MyID(), ID)


def test_id_has_type_and_value_fields():
    ident = ID('abc', '123')

    assert ident.type == 'abc'
    assert ident.value == '123'
