import json
from .fields import FieldABC, IntegerField, ForeignField, DatetimeField
from client import redis_conn as conn
from constants.models import (
    REDIS_PRIMARY_DEFAULT_INCR_KEY, DEFAULT_PRIMARY_KEY, UNIQUE_TOGETHER, CONCRETE_FIELDS, PRIMARY_KEY, UNIQUE_KEYS,
    FOREIGN_KEYS, INDEXES_KEYS, REDIS_UNIQUE_PATTERN,
)


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        _concrete_field = {}
        primary_count = 0
        primary_key = DEFAULT_PRIMARY_KEY
        uniques = []
        if hasattr(new_class.Meta, UNIQUE_TOGETHER):
            uniques = getattr(new_class.Meta, UNIQUE_TOGETHER)
        foreign_keys = []
        for key, value in attrs.items():
            if isinstance(value, FieldABC):
                if value.primary:
                    primary_count += 1
                    primary_key = key
                if value.unique:
                    uniques.append([key])
                assert primary_count < 2
                _concrete_field[key] = value
                if isinstance(value, ForeignField):
                    foreign_keys.append(key)
        if not primary_count:
            _concrete_field[DEFAULT_PRIMARY_KEY] = IntegerField(default=1, primary=True)
        unique_keys = [sorted(item) for item in uniques]
        setattr(new_class, CONCRETE_FIELDS, _concrete_field)
        setattr(new_class, PRIMARY_KEY, primary_key)
        setattr(new_class, UNIQUE_KEYS, unique_keys)
        setattr(new_class, FOREIGN_KEYS, foreign_keys)
        indexes = set()
        if hasattr(new_class.Meta, INDEXES_KEYS):
            for index in getattr(new_class.Meta, INDEXES_KEYS):
                tmp_index = []
                for element in index:
                    tmp_index.append(element)
                    indexes.add(tmp_index)
        for index in unique_keys:
            tmp_index = []
            for element in index:
                tmp_index.append(element)
                indexes.add(tmp_index)
        setattr(new_class, INDEXES_KEYS, indexes)
        return new_class

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)


class BaseModel(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        self.__dict__ = getattr(self, FOREIGN_KEYS)
        for key, value in self.__dict__.items():
            if value.primary:
                if kwargs.get(key):
                    self.__dict__[key].value = kwargs.get(key)
                else:
                    self.__dict__[key].value = self._generate_primary_key()
            elif hasattr(kwargs, key):
                self.__dict__[key].value = kwargs.get(key)
            elif value.required:
                raise AttributeError

    def _hset(self, name, key, value):
        if isinstance(value, dict):
            conn.hset(name, key, json.dumps(value))
        elif isinstance(value, list):
            conn.hset(name, key, json.dumps(value))
        elif isinstance(value, str):
            conn.hset(name, key, value)
        elif isinstance(value, int):
            conn.hset(name, key, value)

    def _retrieve_key_value_from_name(self, name):
        if hasattr(self, name):
            value: FieldABC = getattr(self, name)
            assert isinstance(value, FieldABC)
            return name, value.serializer()
        else:
            return None, None

    def _save_index(self, index_name):
        # 没有级联删除，只能手动删除
        for unique in getattr(self, UNIQUE_KEYS):
            if isinstance(unique, list):
                key_value_pair = []
                for key in unique:
                    key_value_pair.append('-'.join(self._retrieve_key_value_from_name(key)))
                key_value_pair = '-'.join(key_value_pair)
                assert conn.setnx(REDIS_UNIQUE_PATTERN.format(index=index_name, key_value_pair=key_value_pair), 1)
            elif isinstance(unique, str) or isinstance(unique, int):
                key_value_pair = '-'.join(self._retrieve_key_value_from_name(unique))
                assert conn.setnx(REDIS_UNIQUE_PATTERN.format(index=index_name, key_value_pair=key_value_pair), 1)

        for indexes in getattr(self, INDEXES_KEYS):
            for index in indexes:
                key_value_pair = []
                for key in index:
                    key_value_pair.append('-'.join(self._retrieve_key_value_from_name(key)))
                key_value_pair = '-'.join(key_value_pair)
                conn.

    def save(self):
        name = self.__dict__[self.primary_key].value
        self._check_unique(name)
        metadata = self._save_foreign_key(name)
        for key, value in self.__dict__.items():
            if isinstance(value, DatetimeField):
                if not value.get_ori_value() and (value.auto_now or value.auto_now_add):
                    value.value = datetime.now()
                elif value.get_ori_value() and value.auto_now:
                    value.value = datetime.now()
            self._hset(f'{self.Meta.index_name}-{name}', key, value.get_ori_value())
        self._hset(f'{self.Meta.index_name}-{name}', 'metadata', metadata)
        return name

    def update(self):
        raise NotImplementedError

    @staticmethod
    def delete(primary):
        raise NotImplementedError

    @staticmethod
    def filter(**params):
        raise NotImplementedError

    def _generate_primary_key(self):
        if not self.__dict__[self.primary_key].value:
            return conn.incr(REDIS_PRIMARY_DEFAULT_INCR_KEY.format(index=self.Meta.index_name))
        else:
            return self.__dict__[self.primary_key].value

    class Meta:
        unique_together = []
        index_name = 'default'
        indexes = []
