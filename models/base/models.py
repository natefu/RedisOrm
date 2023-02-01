import json
from models.base.fields import FieldABC, IntegerField, ForeignField, DatetimeField
from client import redis_conn as conn
from constants.models import (
    REDIS_PRIMARY_DEFAULT_INCR_KEY, DEFAULT_PRIMARY_KEY, UNIQUE_TOGETHER, CONCRETE_FIELDS, PRIMARY_KEY, UNIQUE_KEYS,
    FOREIGN_KEYS, INDEXES_KEYS, REDIS_UNIQUE_PATTERN, REDIS_INDEX_PATTERN, REDIS_INDEX_PARTITION, REDIS_INDEX_COUNT,
    REDIS_INDEX_POS, BIG_KEY_LIMIT, REDIS_INDEX_PRIMARY_KEY_PATTERN, REDIS_PRIMARY_KEY_PATTERN,
    REDIS_PRIMARY_FOREIGN_VALUE_PATTERN,
)


class Trie:
    def __init__(self):
        self.loopup = {}

    def insert(self, words):
        tree = self.loopup
        for word in words:
            if word not in tree:
                tree[word] = {}
            tree = tree[word]

    def search(self, words):
        tree = self.loopup
        indexes = []
        for word in words:
            if word not in tree:
                return indexes
            indexes.append(word)
            tree = tree[word]
        return indexes


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
            primary_value = IntegerField(primary=True)
            _concrete_field[DEFAULT_PRIMARY_KEY] = primary_value
            setattr(new_class, DEFAULT_PRIMARY_KEY, primary_value)
        unique_keys = [sorted(item) for item in uniques]
        setattr(new_class, CONCRETE_FIELDS, _concrete_field)
        setattr(new_class, PRIMARY_KEY, primary_key)
        setattr(new_class, UNIQUE_KEYS, unique_keys)
        setattr(new_class, FOREIGN_KEYS, foreign_keys)
        indexes = []
        root = Trie()
        if hasattr(new_class.Meta, INDEXES_KEYS):
            for index in getattr(new_class.Meta, INDEXES_KEYS):
                tmp_index = []
                for element in index:
                    tmp_index.append(element)
                    if tmp_index not in index:
                        root.insert(tmp_index.copy())
                        indexes.append(tmp_index.copy())
        for index in unique_keys:
            tmp_index = []
            for element in index:
                tmp_index.append(element)
                if tmp_index not in index:
                    root.insert(tmp_index.copy())
                    indexes.append(tmp_index.copy())
        for index in indexes:
            for element in index:
                value: FieldABC = getattr(new_class, element)
                value.indexes.append(index)
        setattr(new_class, INDEXES_KEYS, indexes)
        return new_class

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)


class BaseModel(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        for key, value in getattr(self, CONCRETE_FIELDS).items():
            if isinstance(value, FieldABC):
                if value.primary:
                    if key in kwargs:
                        value._value = kwargs.get(key)
                    else:
                        value._value = self._generate_primary_key()
                elif key in kwargs:
                    value.value = kwargs.get(key)
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

    def _set_big_keys(self, model_name, primary_key, value):
        position = conn.incr(REDIS_INDEX_POS.format(hash=self.Meta.hash_name, value=value))
        conn.incr(REDIS_INDEX_COUNT.format(hash=self.Meta.hash_name, value=value))
        if position % BIG_KEY_LIMIT == 1:
            partition = conn.incr(REDIS_INDEX_PARTITION.format(hash=self.Meta.hash_name, value=value))
        else:
            partition = conn.get(REDIS_INDEX_PARTITION.format(hash=self.Meta.hash_name, value=value))
        conn.hset(
            REDIS_INDEX_PRIMARY_KEY_PATTERN.format(hash=model_name, value=value, partition=partition),
            f'{self.Meta.hash_name}-{primary_key}', 1
        )
        return position

    def _delete_big_keys(self, model_name, primary_key, value, position):
        conn.decr(REDIS_INDEX_COUNT.format(hash=self.Meta.hash_name, value=value))
        partition = position % BIG_KEY_LIMIT + 1
        conn.hdel(
            REDIS_INDEX_PRIMARY_KEY_PATTERN.format(hash=model_name, value=value, partition=partition),
            f'{self.Meta.hash_name}-{primary_key}'
        )

    def _save_indexes(self, primary_key):
        # 没有级联删除，只能手动删除
        for unique in getattr(self, UNIQUE_KEYS):
            if isinstance(unique, list):
                key_value_pair = []
                for key in unique:
                    key_value_pair.append('-'.join(self._retrieve_key_value_from_name(key)))
                key_value_pair = '-'.join(key_value_pair)
                assert conn.setnx(REDIS_UNIQUE_PATTERN.format(hash=self.Meta.hash_name, value=key_value_pair), 1)
            elif isinstance(unique, str) or isinstance(unique, int):
                key_value_pair = '-'.join(self._retrieve_key_value_from_name(unique))
                assert conn.setnx(REDIS_UNIQUE_PATTERN.format(hash=self.Meta.hash_name, value=key_value_pair), 1)
        index_positions = {}
        for indexes in getattr(self, INDEXES_KEYS):
            key_value_pair = []
            for index in indexes:
                key_value_pair.append('-'.join(self._retrieve_key_value_from_name(index)))
            key_value_pair = '-'.join(key_value_pair)
            position = self._set_big_keys(model_name=self.Meta.hash_name, primary_key=primary_key, value=key_value_pair)
            index_positions[key_value_pair] = position
        return index_positions

    def _delete_indexes(self, primary_key, positions):
        for unique in getattr(self, UNIQUE_KEYS):
            if isinstance(unique, list):
                key_value_pair = []
                for key in unique:
                    key_value_pair.append('-'.join(self._retrieve_key_value_from_name(key)))
                key_value_pair = '-'.join(key_value_pair)
                conn.delete(REDIS_UNIQUE_PATTERN.format(hash=self.Meta.hash_name, value=key_value_pair))
            else:
                key_value_pair = '-'.join(self._retrieve_key_value_from_name(unique))
                conn.delete(REDIS_UNIQUE_PATTERN.format(hash=self.Meta.hash_name, value=key_value_pair))
        if not positions:
            return
        positions = json.loads(positions)
        for indexes in getattr(self, INDEXES_KEYS):
            key_value_pair = []
            for index in indexes:
                key_value_pair.append('-'.join(self._retrieve_key_value_from_name(index)))
            key_value_pair = '-'.join(key_value_pair)
            if positions.get(key_value_pair):
                self._delete_big_keys(
                    model_name=self.Meta.hash_name, primary_key=primary_key, value=key_value_pair,
                    position=positions.get(key_value_pair)
                )

    def _save_foreign_keys(self, primary_key):  # key job_id, foreign_key: process_id
        foreign_positions = {}
        _concrete_fields = getattr(self, CONCRETE_FIELDS)
        for foreign_key in getattr(self, FOREIGN_KEYS):
            _foreign_key: ForeignField = _concrete_fields.get(foreign_key, '')
            assert isinstance(_foreign_key, ForeignField)
            foreign_name = _foreign_key.model.Meta.hash_name
            assert conn.exists(REDIS_PRIMARY_KEY_PATTERN.format(hash=foreign_name, primary=_foreign_key.value))
            position = self._set_big_keys(
                model_name=foreign_name, primary_key=primary_key, value=_foreign_key.value
            )
            foreign_positions[REDIS_PRIMARY_FOREIGN_VALUE_PATTERN.format(
                has=self.Meta.hash_name, foreign=foreign_name, value=_foreign_key.value)] = position
        return foreign_positions

    def _delete_foreign_keys(self, primary_key, positions):
        if not positions:
            return
        positions = json.loads(positions)
        _concrete_fields = getattr(self, CONCRETE_FIELDS)
        for foreign_key in getattr(self, FOREIGN_KEYS):
            _foreign_key: ForeignField = _concrete_fields.get(foreign_key, '')
            if positions.get(_foreign_key.value):
                self._delete_big_keys(
                    model_name=_foreign_key.model.Meta.hash_name, primary_key=primary_key, value=_foreign_key.value,
                    position=positions.get(_foreign_key.value)
                )

    def _save_model(self, name, key, value):
        if isinstance(value, dict):
            conn.hset(name, key, json.dumps(value))
        elif isinstance(value, list):
            conn.hset(name, key, json.dumps(value))
        elif isinstance(value, str):
            conn.hset(name, key, value)
        elif isinstance(value, int):
            conn.hset(name, key, value)

    def save(self):
        _concrete_fields = getattr(self, CONCRETE_FIELDS)
        primary_key = _concrete_fields.get(self.primary_key).value
        primary_name = REDIS_PRIMARY_KEY_PATTERN.format(hash=self.Meta.hash_name, primary=primary_key)
        foreign_keys = self._save_foreign_keys(primary_key=primary_key)
        indexes = self._save_indexes(primary_key=primary_key)
        for key, value in _concrete_fields.items():
            self._save_model(primary_name, key, value.serializer())
        if foreign_keys:
            self._save_model(primary_name, FOREIGN_KEYS, json.dumps(foreign_keys))
        if indexes:
            self._save_model(primary_name, INDEXES_KEYS, json.dumps(indexes))
        for key, value in _concrete_fields.items():
            if isinstance(value, FieldABC):
                value.modified = False
        return primary_key

    def update(self):
        '''
        for key, value in self.__dict__.items():
            if isinstance(value, FieldABC):
                if value.modified:
                    if key in
        '''

    def delete(self):
        _concrete_fields = getattr(self, CONCRETE_FIELDS)
        primary_key = _concrete_fields.get(self.primary_key).value
        primary_name = REDIS_PRIMARY_KEY_PATTERN.format(hash=self.Meta.hash_name, primary=primary_key)
        self._delete_foreign_keys(
            primary_key, conn.hget(primary_name, FOREIGN_KEYS)
        )
        self._delete_indexes(
            primary_key, conn.hget(primary_name, INDEXES_KEYS)
        )
        conn.delete(primary_name)

    @classmethod
    def filter(**params):
        pass

    @classmethod
    def get(cls, id):
        primary_name = REDIS_PRIMARY_KEY_PATTERN.format(hash=cls.Meta.hash_name, primary=id)
        value: dict = conn.hgetall(primary_name)
        if not value:
            raise ValueError
        return cls(**value)

    def _generate_primary_key(self):
        fields = getattr(self, CONCRETE_FIELDS)
        if not fields.get(self.primary_key).value:
            return conn.incr(REDIS_PRIMARY_DEFAULT_INCR_KEY.format(hash=self.Meta.hash_name))
        else:
            return fields.get(self.primary_key).value

    class Meta:
        unique_together = []
        hash_name = 'default'
        indexes = []
