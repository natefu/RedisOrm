import pytest
from exception.exceptions import ValueRequiredException
from models.base.models import BaseModel
from models.base.fields import ForeignField, CharField, BoolField, DatetimeField, ListField, JsonField, IntegerField


class Test(BaseModel):
    key = IntegerField(primary=True)
    uni = IntegerField(unique=True)
    uni1 = IntegerField()
    uni2 = IntegerField()
    ind1 = IntegerField()
    ind2 = IntegerField()
    req = CharField(required=True)

    class Meta:
        hash_name = 'test'
        unique_together = [['uni1', 'uni2']]
        indexes = [['ind1', 'ind2']]


class Process(BaseModel):
    name = CharField()
    version = IntegerField()
    scheme = JsonField()
    deprecated = BoolField(default=False)
    created = DatetimeField(auto_now_add=True)
    updated = DatetimeField(auto_now=True)

    class Meta:
        unique_together = [['name', 'version']]
        indexes = [['name', 'created'], ['name', 'updated'], ['version']]
        hash_name = 'process'


class Test_Model:

    def test_test_model_happy_case(self):
        test = Test(key=1, uni=2, uni1=3, uni2=4, ind1=5, ind2=6, req='a')
        # test value
        assert test.ind1.value == 5
        assert test.ind2.value == 6
        assert test.req.value == 'a'
        # test primary key
        assert test.key.value == 1
        assert test.primary_key == 'key'
        # test unique
        assert test.uni.value == 2
        assert ['uni'] in test.unique_keys
        assert ['uni'] in test.indexes
        # test uni1 and uni2
        assert test.uni1.value == 3
        assert test.uni2.value == 4
        assert ['uni1', 'uni2'] in test.unique_keys
        assert ['uni1'] in test.indexes
        assert ['uni1', 'uni2'] in test.indexes
        # test ind1 and ind2
        assert test.ind1.value == 5
        assert test.ind2.value == 6
        assert ['ind1'] in test.indexes
        assert ['ind1', 'ind2'] in test.indexes
        # test req
        assert test.req.value == 'a'
        assert sorted([['uni'], ['uni1'], ['uni1', 'uni2'], ['ind1'], ['ind1', 'ind2']]) == sorted(test.indexes)

    def test_test_model_bad_case_without_required_value(self):
        with pytest.raises(ValueRequiredException):
            Test(key=1, uni=2, uni1=3, uni2=4, ind1=5, ind2=6)

    def test_test_model_save_process(self):
        from client import conn
        conn.delete_all()
        id_list = {}
        for i in range(30):
            process = Process(name=f'test-{i}', version=1, scheme={'test': i}, deprecated=False)
            id_list[i] = process.save()
        for index, id in id_list.items():
            process = Process.get(id=id)
            assert process.name.value == f'test-{index}'
            assert process.scheme.value == {'test': index}
        processes = Process.filter(version=1)
        assert len(processes) == 30
        for i in range(30):
            process = Process.get(name=f'test-{i}', version=1)
            assert process.id.value == id_list[i]
        conn.delete_all()
