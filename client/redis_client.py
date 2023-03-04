from .client import Client
import redis


redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)


class RedisClient(Client):

    def increase_by_name(self, name):
        result = redis_conn.incr(name)
        print(f'redis: increase {name=} by 1, {result=}')
        return result

    def decrease_by_name(self, name):
        result = redis_conn.decr(name)
        print(f'redis: decrease {name=} by 1, {result=}')
        return result

    def get_by_name(self, name):
        result = redis_conn.get(name)
        print(f'redis: get {name=} , {result=}')
        return result

    def delete_by_name(self, name):
        result = redis_conn.delete(name)
        print(f'redis: delete {name=} , {result=}')
        return result

    def set_value_if_name_not_exists(self, name, value):
        result = redis_conn.setnx(name, value)
        print(f'redis: set {name} = {value} if {name=} does not exist, {result=}')
        return result

    def check_name(self, name):
        result = redis_conn.exists(name)
        print(f'redis: check if {name} exist, {result=}')
        return result

    def set_hash(self, name, key, value):
        result = redis_conn.hset(name, key, value)
        print(f'redis: set hash {name=}, {key=}, {value=}, {result=}')
        return result

    def get_all_hash_by_name(self, name):
        result = redis_conn.hgetall(name)
        print(f'redis: get all hash by {name=}, {result=}')
        return result

    def get_hash_by_name_and_key(self, name, key):
        result = redis_conn.hget(name, key)
        print(f'redis: get hash by {name=} - {key=}, {result=}')
        return result

    def delete_hash_by_name_and_key(self, name, key):
        result = redis_conn.hdel(name, key)
        print(f'redis: delete hash by {name=} - {key=}, {result=}')
        return result

    def check_hash_by_name_and_key(self, name, key):
        result = redis_conn.hexists(name, key)
        print(f'redis: check hash by {name=} - {key=}, {result=}')
        return result

    def delete_all(self):
        start = 1
        results = redis_conn.keys()
        for key in results:
            print(f'{key}, {redis_conn.delete(key)}')
            redis_conn.delete(key)
        while start != 0:
            start, results = redis_conn.scan(start)
            for result in results:
                print(f'{result}, {redis_conn.delete(result)}')
