import json

from client import redis_conn
from constants.models import REDIS_PRIMARY_KEY_PATTERN
from datetime import datetime
from exception.exceptions import InvalidInputException, SystemError


DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'


class FieldMeta(type):
    def __new__(cls, name, bases, attrs):
        return super().__new__(cls, name, bases, attrs)

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)


class FieldABC:
    def __init__(self, default, primary, unique, required):
        if not isinstance(primary, bool) or not isinstance(unique, bool) or not isinstance(required, bool):
            raise InvalidInputException('field input is invalid, please check you field input')
        self.value = default
        self.primary = primary
        self.unique = unique
        self.required = required
        self.indexes = []

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if self.check_value(value):
            self._value = value
        else:
            raise InvalidInputException(value)

    def check_value(self, value):
        return True

    def serialize(self):
        raise NotImplementedError

    def deserialize(self, value):
        raise NotImplementedError

    def __str__(self):
        return self.serialize()


class IntegerField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default=0, primary=False, unique=False, required=False):
        super().__init__(default, primary, unique, required)

    @property
    def value(self):
        assert self._value is not None
        return self._value

    @value.setter
    def value(self, value):
        if isinstance(value, int):
            self._value = value
        elif isinstance(value, str):
            try:
                self._value = int(value)
            except ValueError:
                raise InvalidInputException(f'{value} should be int')
        else:
            raise InvalidInputException(f'{value} should be int')

    def check_value(self, value):
        return isinstance(value, int) or value.isdigit()

    def serialize(self):
        return str(self.value)

    def deserialize(self, value):
        self.value = value


class BoolField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default=False, primary=False, unique=False, required=False):
        super().__init__(default, primary, unique, required)

    @property
    def value(self):
        assert self._value is not None
        return self._value

    @value.setter
    def value(self, value):
        if isinstance(value, bool):
            self._value = value
        elif value in [0, '0', 'false', 'False']:
            self._value = False
        elif value in [1, '1', 'true', 'True']:
            self._value = True
        else:
            raise InvalidInputException(f'{value} should be boolean')

    def check_value(self, value):
        return isinstance(value, bool)

    def serialize(self):
        return '1' if self._value else '0'

    def deserialize(self, value):
        self.value = value


class CharField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default='', primary=False, unique=False, required=False):
        super().__init__(default, primary, unique, required)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = str(value)

    def serialize(self):
        return self._value

    def deserialize(self, value):
        self.value = value


class DatetimeField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default=datetime.now(), auto_now_add: bool = False, auto_now: bool = False, required=False):
        self._auto_now_add = auto_now_add
        self._auto_now = auto_now
        super().__init__(default, False, False, required)

    @property
    def auto_now_add(self):
        return self._auto_now_add

    @auto_now_add.setter
    def auto_now_add(self, auto_now_add):
        self._auto_now_add = auto_now_add

    @property
    def auto_now(self):
        return self._auto_now

    @auto_now.setter
    def auto_now(self, auto_now):
        self._auto_now = auto_now

    @property
    def value(self):
        if self.auto_now:
            return datetime.now()
        if self._value:
            try:
                return datetime.strptime(self._value, DATETIME_PATTERN)
            except ValueError:
                raise SystemError(f'{self._value} should be datetime str')
        if self.auto_now_add:
            return datetime.now()

    @value.setter
    def value(self, value):
        if self.auto_now:
            self._value = datetime.now().strftime(DATETIME_PATTERN)
        elif isinstance(value, datetime):
            self._value = value.strftime(DATETIME_PATTERN)
        else:
            if not value and self.auto_now_add:
                self._value = datetime.now().strftime(DATETIME_PATTERN)
            else:
                raise InvalidInputException(f'{value} should be datetime')

    def serialize(self):
        return self.value.strftime(DATETIME_PATTERN)

    def deserialize(self, value):
        try:
            self.value = datetime.strptime(value, DATETIME_PATTERN)
        except ValueError:
            raise InvalidInputException(f'{value} should be datetime')


class ListField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default: list = list(), required=False):
        super().__init__(default, False, False, required)

    @property
    def value(self):
        try:
            return json.loads(self._value)
        except:
            raise SystemError(f'{self._value} should be json str')

    @value.setter
    def value(self, value: list):
        if not isinstance(value, list):
            raise InvalidInputException(f'{value} should be list')
        try:
            self._value = json.dumps(value)
        except:
            raise InvalidInputException(f'{value} should be list')

    def serialize(self):
        return self._value

    def deserialize(self, value):
        try:
            json_value = json.loads(value)
            assert isinstance(json_value, list)
        except:
            raise InvalidInputException(f'{value} should be dumped list')
        self._value = value


class JsonField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default: dict = dict(), required=False):
        super().__init__(default, False, False, required)

    @property
    def value(self):
        try:
            return json.loads(self._value)
        except:
            raise SystemError(f'{self._value} should be json str')

    @value.setter
    def value(self, value: [dict, str]):
        if isinstance(value, dict):
            try:
                self._value = json.dumps(value)
            except:
                raise InvalidInputException(f'{value} should be dict or dict str')
        else:
            raise InvalidInputException(f'{value} should be dict or dict str')

    def serialize(self):
        return self._value

    def deserialize(self, value):
        try:
            json_value = json.loads(value)
            assert isinstance(json_value, dict)
        except:
            raise InvalidInputException(f'{value} should be dumped dict')
        self._value = value


class ForeignField(FieldABC, metaclass=FieldMeta):
    def __init__(self, model, unique=False, required=False):
        super().__init__(None, False, unique, required)
        self.model = model

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        from .models import BaseModel
        if not value:
            self._value = None
        elif isinstance(value, BaseModel):
            self._value = value.__dict__.get(self.model.primary_key)
        else:
            assert isinstance(value, int) or isinstance(value, str)
            if self.check_value(value):
                self._value = value
            else:
                raise InvalidInputException(f'{value} should be other model primary key')

    def check_value(self, value):
        if value is None:
            return True
        return redis_conn.exists(REDIS_PRIMARY_KEY_PATTERN.format(hash=self.model.hash_name, primary=value))

    def serialize(self):
        return self.value

    def deserialize(self, value):
        self.value = value
