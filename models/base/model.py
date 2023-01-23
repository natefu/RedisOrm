from .field import FieldABC, IntegerField
from client import redis_conn

class ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        _concrete_field = {}
        primary_count = 0
        for key, value in attrs.items():
            if isinstance(value, FieldABC):
                if value.primary:
                    primary_count += 1
                assert primary_count < 2
                _concrete_field[key] = value
        if not primary_count:
            _concrete_field['id'] = IntegerField(default=1, primary=True)
        setattr(new_class, 'concrete_field', _concrete_field)
        return new_class

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)


class BaseModel(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        self.__dict__ = getattr(self, 'concrete_field')
        for key, value in kwargs.items():
            if hasattr(self.__dict__, key):
                self.__dict__[key].value = value

    def save(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    @staticmethod
    def delete(primary):
        raise NotImplementedError

    @staticmethod
    def filter(**params):
        raise NotImplementedError

    class Meta:
        unique_together = []
        index_name = 'default'
        indexes = []
