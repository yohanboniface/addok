import marshal

from addok.config import config
from addok.db import DB
from addok.helpers import import_by_path, keys


class DocumentStorage:

    def get(self, *keys):
        pipe = DB.pipeline(transaction=False)
        for key in keys:
            pipe.get(key)
        for doc in pipe.execute():
            yield marshal.loads(doc)

    def add(self, *docs):
        pipe = DB.pipeline(transaction=False)
        for key, doc in docs:
            pipe.set(key, marshal.dumps(doc))
            yield doc
        pipe.execute()

    def remove(self, *docs):
        pipe = DB.pipeline(transaction=False)
        for key, doc in docs:
            pipe.delete(key)
            yield doc
        pipe.execute()


class DSProxy:
    instance = None

    def __getattr__(self, name):
        return getattr(self.instance, name)


DS = DSProxy()


@config.on_load
def on_load():
    DS.instance = import_by_path(config.DOCUMENT_STORAGE)()


def store_documents(docs):
    count = 0
    to_add = []
    to_remove = []
    for doc in docs:
        key = keys.document_key(doc['id'])
        if doc.get('_action') in ['delete', 'update']:
            to_remove.append((key, doc))
        if doc.get('_action') in ['index', 'update', None]:
            to_add.append((key, doc))
        count += 1
        if count % config.STORAGE_IMPORT_CHUNK_SIZE == 0:
            if to_add:
                yield from DS.add(*to_add)
                to_add = []
            if to_remove:
                yield from DS.remove(*to_remove)
                to_remove = []
    if to_add:
        yield from DS.add(*to_add)
    if to_remove:
        yield from DS.remove(*to_remove)
