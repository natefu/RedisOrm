import json
import pytest
from exception.exceptions import InvalidInputException
from datetime import datetime
from unittest import mock
from models.base.fields import (
    FieldABC, IntegerField, CharField, BoolField, DatetimeField, ListField, JsonField, ForeignField, DATETIME_PATTERN
)
from models.base.models import BaseModel


class Test(BaseModel):
    name = CharField()

    class Meta:
        hash_name = 'test'


class Test2(BaseModel):
    name = CharField()

    class Meta:
        hash_name = 'test'


class Test3:
    pass


class Test_Field:

    def test_default_initial_field(self):
        with pytest.raises(InvalidInputException):
            FieldABC(default="hello_world", primary='test', unique='test', required='test')

    def test_default_integer_field_happy_case(self):
        integer_field = IntegerField()
        assert integer_field.value == 0
        assert integer_field.serialize() == '0'
        assert not integer_field.unique
        assert not integer_field.primary
        assert not integer_field.required

    def test_int_integer_field_happy_case(self):
        integer_field = IntegerField(1)
        assert integer_field.value == 1
        assert integer_field.serialize() == '1'
        assert not integer_field.unique
        assert not integer_field.primary
        assert not integer_field.required

    def test_string_integer_field_happy_case(self):
        integer_field = IntegerField('1')
        assert integer_field.value == 1
        assert integer_field.serialize() == '1'
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

    def test_bool_field_happy_case(self):
        bool_field = BoolField()
        assert not bool_field.value
        assert bool_field.serialize() == '0'

    def test_bool_field_happy_case_with_true_value(self):
        for value in [1, '1', 'True', 'true', True]:
            bool_field = BoolField(value)
            assert bool_field.value
            assert bool_field.serialize() == '1'

    def test_bool_field_happy_case_with_false_value(self):
        for value in [0, '0', 'False', 'false', False]:
            bool_field = BoolField(value)
            assert not bool_field.value
            assert bool_field.serialize() == '0'

    def test_bool_field_bad_case_with_invalid_value(self):
        for value in [3, '3', 'happy_case']:
            with pytest.raises(InvalidInputException):
                BoolField(value)

    def test_bool_field_happy_case_with_set_valid_field(self):
        bool_field = BoolField()
        assert not bool_field.value
        for value in [1, '1', 'True', 'true', True]:
            bool_field.value = value
            assert bool_field.value
            assert bool_field.serialize() == '1'

    def test_bool_field_base_case_with_set_invalid_field(self):
        bool_field = BoolField()
        assert not bool_field.value
        for value in [3, '3', 'happy_case']:
            with pytest.raises(InvalidInputException):
                bool_field.value = value

    def test_bool_field_happy_case_with_deserialize_valid_field(self):
        bool_field = BoolField()
        assert not bool_field.value
        for value in [1, '1', 'True', 'true', True]:
            bool_field.deserialize(value)
            assert bool_field.value
            assert bool_field.serialize() == '1'

    def test_bool_field_base_case_with_deserialize_invalid_field(self):
        bool_field = BoolField()
        assert not bool_field.value
        for value in [3, '3', 'happy_case']:
            with pytest.raises(InvalidInputException):
                bool_field.deserialize(value)

    def test_char_field_happy_case(self):
        char_field = CharField()
        assert char_field.value == char_field.serialize() == ''

    def test_char_field_happy_case_with_any_input(self):
        for value, d_value in [(1, '1'), (True, 'True'), ('hello', 'hello'), (list, "<class 'list'>"),
                               (set, "<class 'set'>"), (map, "<class 'map'>"), (["a"], "['a']"), (None, 'None')]:
            char_field = CharField(value)
            assert char_field.value == char_field.serialize() == d_value

    def test_char_field_happy_case_with_set_value(self):
        char_field = CharField()
        assert char_field.value == char_field.serialize() == ''
        for value, d_value in [(1, '1'), (True, 'True'), ('hello', 'hello'), (list, "<class 'list'>"),
                               (set, "<class 'set'>"), (map, "<class 'map'>"), (["a"], "['a']"), (None, 'None')]:
            char_field.value = value
            assert char_field.value == char_field.serialize() == d_value

    def test_char_field_happy_case_with_deserialize_value(self):
        char_field = CharField()
        assert char_field.value == char_field.serialize() == ''
        for value, d_value in [(1, '1'), (True, 'True'), ('hello', 'hello'), (list, "<class 'list'>"),
                               (set, "<class 'set'>"), (map, "<class 'map'>"), (["a"], "['a']"), (None, 'None')]:
            char_field.deserialize(value)
            assert char_field.value == char_field.serialize() == d_value

    def test_date_time_field_happy_case(self):
        datetime_field = DatetimeField()
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_happy_case_with_auto_now_add(self):
        previous_datetime_string = '1993-03-02 13:24:32'
        previous_datetime = datetime.strptime(previous_datetime_string, DATETIME_PATTERN)
        datetime_field = DatetimeField(previous_datetime, auto_now_add=True)
        assert datetime_field.value == previous_datetime
        assert datetime_field.serialize() == previous_datetime_string

    def test_date_time_field_happy_case_with_auto_now_add_none_value(self):
        datetime_field = DatetimeField(None, auto_now_add=True)
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_happy_case_with_auto_now_add_empty_value(self):
        datetime_field = DatetimeField('', auto_now_add=True)
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_happy_case_with_auto_now(self):
        previous_datetime_string = '1993-03-02 13:24:32'
        previous_datetime = datetime.strptime(previous_datetime_string, DATETIME_PATTERN)
        datetime_field = DatetimeField(previous_datetime, auto_now=True)
        assert datetime_field.value != previous_datetime
        assert datetime_field.serialize() != previous_datetime_string
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_happy_case_with_auto_now_none_value(self):
        datetime_field = DatetimeField(None, auto_now=True)
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_happy_case_with_auto_now_empty_value(self):
        datetime_field = DatetimeField('', auto_now=True)
        assert isinstance(datetime_field.value, datetime)
        assert isinstance(datetime.strptime(datetime_field.serialize(), DATETIME_PATTERN), datetime)

    def test_date_time_field_bad_case_with_invalid_input(self):
        with pytest.raises(InvalidInputException):
            DatetimeField('bad_case')

    def test_date_time_field_happy_case_with_set_valid_value(self):
        datetime_field = DatetimeField()
        previous_datetime_string = '1993-03-02 13:24:32'
        previous_datetime = datetime.strptime(previous_datetime_string, DATETIME_PATTERN)
        datetime_field.value = previous_datetime
        assert datetime_field.value == previous_datetime
        assert datetime_field.serialize() == previous_datetime_string

    def test_date_time_field_happy_case_with_serialize_valid_value(self):
        datetime_field = DatetimeField()
        previous_datetime_string = '1993-03-02 13:24:32'
        previous_datetime = datetime.strptime(previous_datetime_string, DATETIME_PATTERN)
        datetime_field.deserialize(previous_datetime_string)
        assert datetime_field.value == previous_datetime
        assert datetime_field.serialize() == previous_datetime_string

    def test_date_time_field_bad_case_with_serialize_invalid_value(self):
        datetime_field = DatetimeField()
        previous_datetime_string = '1993-03fasdfas-02 13:24:2'
        with pytest.raises(InvalidInputException):
            datetime_field.deserialize(previous_datetime_string)

    def test_list_field_happy_case(self):
        list_field = ListField()
        assert list_field.value == []
        assert list_field.serialize() == '[]'

    def test_list_field_happy_case_with_valid_input(self):
        input_list = [1, 'char']
        list_field = ListField(input_list)
        assert list_field.value == input_list
        assert list_field.serialize() == json.dumps(input_list)

    def test_list_field_bad_case_with_invalid_input(self):
        with pytest.raises(InvalidInputException):
            ListField('a')

    def test_list_field_happy_case_with_set_valid_input(self):
        list_field = ListField()
        input_list = [1, 'char']
        list_field.value = input_list
        assert list_field.value == input_list
        assert list_field.serialize() == json.dumps(input_list)

    def test_list_field_bad_case_with_set_invalid_input(self):
        list_field = ListField()
        with pytest.raises(InvalidInputException):
            list_field.value = 'a'

    def test_list_field_happy_case_with_deserialize_valid_input(self):
        list_field = ListField()
        input_list = [1, 'char']
        list_field.deserialize(json.dumps(input_list))
        assert list_field.value == input_list
        assert list_field.serialize() == json.dumps(input_list)

    def test_list_field_happy_case_with_deserialize_invalid_input(self):
        list_field = ListField()
        input_list = {1: "valid"}
        with pytest.raises(InvalidInputException):
            list_field.deserialize(json.dumps(input_list))

    def test_list_field_happy_case_with_deserialize_invalid_input_2(self):
        list_field = ListField()
        input_list = {1: "valid"}
        with pytest.raises(InvalidInputException):
            list_field.deserialize(input_list)

    def test_json_field_happy_case(self):
        json_field = JsonField()
        assert json_field.value == {}
        assert json_field.serialize() == '{}'

    def test_json_field_happy_case_with_valid_input(self):
        input_dict = {'1': 'char'}
        json_field = JsonField(input_dict)
        assert json_field.value == input_dict
        assert json_field.serialize() == json.dumps(input_dict)

    def test_json_field_bad_case_with_invalid_input(self):
        with pytest.raises(InvalidInputException):
            JsonField('a')

    def test_json_field_happy_case_with_set_valid_input(self):
        json_field = JsonField()
        input_json = {'1': 'char'}
        json_field.value = input_json
        assert json_field.value == input_json
        assert json_field.serialize() == json.dumps(input_json)

    def test_json_field_bad_case_with_set_invalid_input(self):
        json_field = JsonField()
        with pytest.raises(InvalidInputException):
            json_field.value = 'a'

    def test_json_field_happy_case_with_deserialize_valid_input(self):
        json_field = JsonField()
        input_json = {'1': 'char'}
        json_field.deserialize(json.dumps(input_json))
        assert json_field.value == input_json
        assert json_field.serialize() == json.dumps(input_json)

    def test_json_field_happy_case_with_deserialize_invalid_input(self):
        json_field = JsonField()
        input_list = [1, "valid"]
        with pytest.raises(InvalidInputException):
            json_field.deserialize(json.dumps(input_list))

    def test_json_field_happy_case_with_deserialize_invalid_input_2(self):
        json_field = JsonField()
        input_dict = {'1': "valid"}
        with pytest.raises(InvalidInputException):
            json_field.deserialize(input_dict)

    def test_foreign_field_happy_case(self):
        test = Test(name='test')
        test.id = 1
        foreign_field = ForeignField(test)
        assert foreign_field.model == Test
        assert foreign_field._value == 1
        assert foreign_field.serialize() == 1

    @mock.patch.object(ForeignField, 'check_value')
    def test_foreign_field_happy_case_with_set_valid_input(self, mock_class):
        test = Test(name='test')
        test.save()
        test.id = 1
        test2 = Test(name='test')
        test2.save()
        test2.id = 2
        foreign_field = ForeignField(test)
        mock_class.return_value = 1
        for (value, result) in [(2, 2), ('2', '2'), (test2, 2)]:
            foreign_field.value = value
            assert foreign_field.model == Test
            assert foreign_field._value == result
            assert foreign_field.serialize() == result

    def test_foreign_field_happy_case_with_set_invalid_input(self):
        test = Test(name='test')
        test.save()
        test.id = 1
        foreign_field = ForeignField(test)
        test2 = Test2(name='test')
        test2.save()
        test2.id = 1
        test3 = Test3()
        for value in [[2], test2, test3]:
            with pytest.raises(InvalidInputException):
                foreign_field.value = value

    @mock.patch.object(ForeignField, 'check_value')
    def test_foreign_field_happy_case_with_deserialize_valid_value(self, mock_class):
        test = Test(name='test')
        test.save()
        test.id = 1
        test2 = Test(name='test')
        test2.save()
        test2.id = 2
        foreign_field = ForeignField(test)
        mock_class.return_value = 1
        for (value, result) in [(2, 2), ('2', '2'), (test2, 2)]:
            foreign_field.deserialize(value)
            assert foreign_field.model == Test
            assert foreign_field._value == result
            assert foreign_field.serialize() == result

    def test_foreign_field_happy_case_with_deserialize_invalid_input(self):
        test = Test(name='test')
        test.save()
        test.id = 1
        foreign_field = ForeignField(test)
        test2 = Test2(name='test')
        test2.save()
        test2.id = 1
        test3 = Test3()
        for value in [[2], test2, test3]:
            with pytest.raises(InvalidInputException):
                foreign_field.deserialize(value)

    def test_foreign_field_bad_case_without_model(self):
        with pytest.raises(InvalidInputException):
            ForeignField(1)

    @mock.patch.object(ForeignField, 'check_value')
    def test_foreign_field_bad_case_primary_key_not_in_redis(self, mock_class):
        test = Test(name='test')
        test.save()
        test.id = 1
        with pytest.raises(InvalidInputException):
            mock_class.return_value = 0
            ForeignField(test)

    @mock.patch.object(ForeignField, 'check_value')
    def test_foreign_field_bad_case_set_primary_key_not_in_redis(self, mock_class):
        test = Test(name='test')
        test.save()
        test.id = 1
        test2 = Test(name='test')
        test2.save()
        test2.id = 2
        foreign_field = ForeignField(test)
        mock_class.return_value = 0
        for (value, result) in [(2, 2), ('2', '2'), (test2, 2)]:
            with pytest.raises(InvalidInputException):
                foreign_field.value = value

    @mock.patch.object(ForeignField, 'check_value')
    def test_foreign_field_bad_case_primary_deserialize_key_not_in_redis(self, mock_class):
        test = Test(name='test')
        test.save()
        test.id = 1
        test2 = Test(name='test')
        test2.save()
        test2.id = 2
        foreign_field = ForeignField(test)
        mock_class.return_value = 0
        for (value, result) in [(2, 2), ('2', '2'), (test2, 2)]:
            with pytest.raises(InvalidInputException):
                foreign_field.deserialize(value)
