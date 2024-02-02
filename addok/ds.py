from addok.config import config
from addok.db import DB, RedisProxy
from addok.helpers import keys


class RedisStore:
    def fetch(self, *keys):
        pipe = _DB.pipeline(transaction=False)
        for key in keys:
            pipe.get(key)
        for key, doc in zip(keys, pipe.execute()):
            if doc is not None:
                yield key, doc

    def upsert(self, *docs):
        pipe = _DB.pipeline(transaction=False)
        for key, blob in docs:
            pipe.set(key, blob)
        pipe.execute()

    def remove(self, *keys):
        pipe = _DB.pipeline(transaction=False)
        for key in keys:
            pipe.delete(key)
        pipe.execute()

    def flushdb(self):
        _DB.flushdb()


class DSProxy:
    instance = None

    def __getattr__(self, name):
        return getattr(self.instance, name)


_DB = RedisProxy()
DS = DSProxy()


@config.on_load
def on_load():
    DS.instance = config.DOCUMENT_STORE()
    # Do not create connection if not using this store class.
    if config.DOCUMENT_STORE == RedisStore:
        params = config.REDIS.copy()
        params.update(config.REDIS.get("documents", {}))
        _DB.connect(
            host=params.get("host"),
            port=params.get("port"),
            db=params.get("db"),
            password=params.get("password"),
            unix_socket_path=params.get("unix_socket_path"),
            ssl=params.get("ssl"), # default : False,
        )


def store_documents(docs):
    to_upsert = []
    to_remove = []
    for doc in docs:
        if not doc:
            continue
        if config.ID_FIELD not in doc:
            doc[config.ID_FIELD] = DB.next_id()
        key = keys.document_key(doc[config.ID_FIELD])
        if doc.get("_action") in ["delete", "update"]:
            to_remove.append(key)
        if doc.get("_action") in ["index", "update", None]:
            to_upsert.append((key, config.DOCUMENT_SERIALIZER.dumps(doc)))
        yield doc
    if to_remove:
        DS.remove(*to_remove)
    if to_upsert:
        DS.upsert(*to_upsert)


def get_document(key):
    results = DS.fetch(key)
    try:
        _, doc = next(results)
    except StopIteration:
        return None
    return config.DOCUMENT_SERIALIZER.loads(doc)


def get_documents(*keys):
    for id_, blob in DS.fetch(*keys):
        yield id_, config.DOCUMENT_SERIALIZER.loads(blob)
