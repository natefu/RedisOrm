from abc import abstractmethod, ABCMeta


class Client(metaclass=ABCMeta):

    @abstractmethod
    def increase_by_name(self, name):
        pass

    @abstractmethod
    def decrease_by_name(self, name):
        pass

    @abstractmethod
    def get_by_name(self, name):
        pass

    @abstractmethod
    def delete_by_name(self, name):
        pass

    @abstractmethod
    def set_value_if_name_not_exists(self, name, value):
        pass

    @abstractmethod
    def check_name(self, name):
        pass

    @abstractmethod
    def set_hash(self, name, key, value):
        pass

    @abstractmethod
    def get_all_hash_by_name(self, name):
        pass

    @abstractmethod
    def get_hash_by_name_and_key(self, name, key):
        pass

    @abstractmethod
    def delete_hash_by_name_and_key(self, name, key):
        pass

    @abstractmethod
    def check_hash_by_name_and_key(self, name, key):
        pass

    @abstractmethod
    def delete_all(self):
        pass
