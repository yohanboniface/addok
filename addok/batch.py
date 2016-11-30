from datetime import timedelta
import json
import os.path
import sys

from addok.config import config
from addok.helpers import iter_pipe, parallelize, yielder, Bar
from addok.helpers.index import deindex_document, index_document


def run(args):
    if args.filepath:
        for path in args.filepath:
            process_file(path)
    elif not sys.stdin.isatty():  # Any better way to check for stdin?
        process_stdin(sys.stdin)


def register_command(subparsers):
    parser = subparsers.add_parser('batch', help='Batch import documents')
    parser.add_argument('filepath', nargs='*',
                        help='Path to file to process')
    parser.set_defaults(func=run)


def preprocess_batch(d):
    config.INDEX_EDGE_NGRAMS = False  # Run command "ngrams" instead.
    return iter_pipe(d, config.BATCH_PROCESSORS)


def process_file(filepath):
    print('Import from file', filepath)
    _, ext = os.path.splitext(filepath)
    if not os.path.exists(filepath):
        sys.stderr.write('File not found: {}'.format(filepath))
        sys.exit(1)
    if ext == '.msgpack':
        import msgpack  # We don't want to make it a required dependency.
        with open(filepath, mode='rb') as f:
            batch(preprocess_batch(msgpack.Unpacker(f, encoding='utf-8')))
    else:
        with open(filepath) as f:
            batch(preprocess_batch(f))


def process_stdin(stdin):
    print('Import from stdin')
    batch(preprocess_batch(stdin))


@yielder
def to_json(row):
    try:
        return json.loads(row)
    except ValueError:
        return None


def process_documents(docs):
    from addok.db import DB
    pipe = DB.pipeline(transaction=False)
    for doc in iter_pipe(docs, config.DOCUMENT_PROCESSORS):
        if doc.get('_action') in ['delete', 'update']:
            deindex_document(DB, doc['id'])
        if doc.get('_action') in ['index', 'update', None]:
            index_document(pipe, doc)
    pipe.execute()
    return docs


def batch(iterable):
    parallelize(process_documents, iterable, chunk_size=1000,
                prefix='Importing…', throttle=timedelta(seconds=1))
