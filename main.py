# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from models.base.fields import *
from models.base.models import *


class Process(BaseModel):
    name = CharField()
    version = IntegerField()
    scheme = JsonField()
    deprecated = BoolField(default=False)
    created = DatetimeField(auto_now_add=True)
    updated = DatetimeField(auto_now=True)

    class Meta:
        unique_together = [['name', 'version']]
        indexes = [['name', 'created'], ['name', 'updated']]
        hash_name = 'process'

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    '''
    for i in range(30):
        process = Process(name=f'test-{i}', version=3, scheme={'test': i}, deprecated=False)
        process.save()

    for i in range(30):
        process = Process.get(id=51+i)
        print(process.id)
        print(process.name)
        print(process.version)
    process = Process.get(name='test-1', version=4)
    process_1 = Process.get(id=82)
    process_2 = Process.get(id=83)
    print(process_2.name)
    print(process_1.name)
    
    processes = Process.filter(name='test-1', deprecated=False)
    for process in processes:
        print(process.id)
    '''
    process_1 = Process.get(id=82)
    process = Process(name='test-31', version=3, scheme={'test': 31}, deprecated=False)

    print(process.id)
    process.save()
    print(process.id)
    print(process_1.__dict__)