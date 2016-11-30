import redis

from addok.config import config


class DBRedis:
    instance = None
    Error = redis.RedisError

    def connect(self, *args, **kwargs):
        self.instance = redis.StrictRedis(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.instance, name)


DB = DBRedis()


@config.on_load
def connect():
    DB.connect(**config.REDIS)
