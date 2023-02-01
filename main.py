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
    #process = Process(name='test', version=9, scheme={}, deprecated=False)
    #print(process.save())
    process = Process.get(16)
    print(process.name)
    process.delete()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
