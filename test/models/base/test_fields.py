import pytest
from exception.exceptions import InvalidInputException
from models.base.fields import (
    FieldABC, IntegerField, CharField, BoolField, DatetimeField, ListField, JsonField, ForeignField
)


class Test_Field:

    def test_default_initial_field(self):
        with pytest.raises(InvalidInputException):
            FieldABC(default="hello_world", primary='test', unique='test', required='test')

    def test_default_initial_method(self):
        integer_field = IntegerField()
        assert integer_field.value == 0
        assert integer_field.serialize() == '0'
        assert not integer_field.unique
        assert not integer_field.primary
        assert not integer_field.required

    def test_initial_integer_field_with_not_int(self):
        with pytest.raises(InvalidInputException):
            IntegerField("hello_world")

    def test_initial_integer_field_with_not_digit(self):
        integer_field = IntegerField("1")
        assert integer_field.value == 1

    def test_integer_field_set_value_with_digit(self):
        integer_field = IntegerField(1)
        integer_field.value = '2'
        assert integer_field.value == 2

    def test_integer_field_serialize(self):
        integer_field = IntegerField(1)
        assert integer_field.serialize() == '1'

    def test_integer_field_deserialize(self):
        integer_field = IntegerField()
        integer_field.deserialize('3')
        assert integer_field.value == 3
        assert integer_field.serialize() == '3'
