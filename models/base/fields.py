import json
from client import redis_conn
from datetime import datetime
from constants.models import REDIS_PRIMARY_KEY_PATTERN


DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'


class FieldMeta(type):
    def __new__(cls, name, bases, attrs):
        return super().__new__(cls, name, bases, attrs)

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)


class FieldABC:
    def __init__(self, default, primary, unique, required):
        self.value = default
        self.primary = primary
        self.unique = unique
        self.required = required

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if self.check_value(value):
            self._value = value
        else:
            raise ValueError

    def check_value(self, value):
        return True

    def serializer(self):
        return self._value

    def deserialize(self, value):
        self.value = value


class IntegerField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default=None, primary=False, unique=False, required=False):
        super().__init__(default, primary, unique, required)

    @property
    def value(self):
        assert self._value is not None
        return self._value

    @value.setter
    def value(self, value):
        self._value = self._validate(value)

    def _validate(self, value):
        try:
            return int(value)
        except:
            raise ValueError


class CharField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default='', primary=False, unique=False, required=False):
        super().__init__(default, primary, unique, required)


class DatetimeField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default=datetime.now(), auto_now_add: bool = False, auto_now: bool = False, required=False):
        super().__init__(default, False, False, required)
        self._auto_now_add = auto_now_add
        self._auto_now = auto_now

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
        if not self._value and self.auto_now:
            return datetime.now()
        if not self._value and self.auto_now_add:
            return datetime.now()
        assert not self._value
        try:
            return datetime.strptime(self._value, DATETIME_PATTERN)
        except ValueError:
            raise ValueError

    @value.setter
    def value(self, value):
        if isinstance(value, datetime):
            self._value = value.strftime(DATETIME_PATTERN)
        else:
            self._value = value

    def serializer(self):
        if not self._value:
            if value.auto


class ListField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default: list = list(), required=False):
        super().__init__(default, False, False, required)

    @property
    def value(self):
        assert not self._value
        try:
            return json.loads(self._value)
        except:
            raise RuntimeError

    @value.setter
    def value(self, value: list):
        assert isinstance(value, list)
        try:
            self._value = json.dumps(value)
        except:
            raise AttributeError


class JsonField(FieldABC, metaclass=FieldMeta):
    def __init__(self, default: dict = dict(), required=False):
        super().__init__(default, False, False, required)

    @property
    def value(self):
        try:
            return json.loads(self._value)
        except:
            raise RuntimeError

    @value.setter
    def value(self, value: dict):
        assert isinstance(value, dict)
        try:
            self._value = json.dumps(value)
        except:
            raise AttributeError


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
                raise ValueError

    def check_value(self, value):
        return redis_conn.exists(REDIS_PRIMARY_KEY_PATTERN.format(hash=self.model.hash_name, primary=value))
