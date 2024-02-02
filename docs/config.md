# Configuring Addok

By default, Addok is configured for a French database of addresses from France
*(It may be because it has been initially coded in Paris… ;) )*

But certainly your needs are different, and even if you deal with French addresses
you may want to define **which fields are indexed** or **which filters are
available** for example.

*See also [Redis Tuning](redis.md).*

## Registering your custom config file

An Addok config file is simply a Python file that define some keys. The default
location is `/etc/addok/addok.conf`. But it can be anywhere else in your system,
and you need to define an environment variable that points to it if you want
a special location:

    export ADDOK_CONFIG_MODULE=path/to/local.py

You can also use the `--config` argument when running the `addok` command line.

The default config file is in `addok/config/default.py`.

## Using env vars

Any specific config key can be declared using an env var, using the key itself,
prefixed by `ADDOK_`. For example, to override `BATCH_WORKERS`, one may do
something like this:

    ADDOK_BATCH_WORKERS=12 addok batch

## Environment settings

Some settings are used to define how addok will deal with the system it's
installed on.

#### REDIS (dict)
Defines the Redis database settings:

```
    REDIS = {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    }
```    

By default, when using the `RedisStore` for documents, indexes and documents
will be stored in two different Redis databases.
You can control those details by using `indexes` and/or `documents`
subdictionnaries, for example:

```
    REDIS = {
        'host': 'myhost',
        'port': 6379,
        'indexes': {
            'db': 11,
        },
        'documents': {
            'db': 12,
        }
    }
```

If your hosts are different, you can define them like this:

```
    REDIS = {
        'port': 6379,
        'indexes': {
            'host': 'myhost1',
            'db': 11,
        },
        'documents': {
            'db': 12,
            'host': 'myhost2',
        }
    }
```


And of course, same for the port.

To use Redis through a Unix socket, use `unix_socket_path` key.

To use external redis with ssl connection you can add the 'ssl' parameters :

```
    REDIS = {
        'host': 'myhost',
        'port': 6379,
        'ssl' : True,
        'indexes': {
            'db': 11,
        },
        'documents': {
            'db': 12,
        }
    }
```

If you want to use environement variables instead. Remove entry 'REDIS' in configuration file and use ENVIRONEMENTS variables :


| Variable           | require | Default  | Descripion             | example       |
|--------------------|---------|----------|------------------------|---------------|
| REDIS_HOST         | true    | locahost | Redis host             |               |
| REDIS_PORT         | true    | 6379     | Redis port             |               |
| REDIS_SOCKET       | false   |          |                        |               |
| REDIS_DB_INDEXES   | true    | 0        |                        |               |
| REDIS_DB_DOCUMENTS | true    | 1        |                        |               |
| REDIS_SSL          | false   |          |  Acive SSL             | True          |



    "host": os.environ.get("REDIS_HOST") or "localhost",
    "port": os.environ.get("REDIS_PORT") or 6379,
    "unix_socket_path": os.environ.get("REDIS_SOCKET"),
    "indexes": {
        "db": os.environ.get("REDIS_DB_INDEXES") or 0,
    },
    "documents": {
        "db": os.environ.get("REDIS_DB_DOCUMENTS") or 1,
    },
    "ssl": os.environ.get("REDIS_SSL") == 'True',


#### LOG_DIR (path)
Path to the directory Addok will write its log and history files. Can also
be overridden from the environment variable `ADDOK_LOG_DIR`.

    LOG_DIR = 'path/to/dir'

This setting defaults to the root folder of the addok package.


## Basic settings

A bunch of settings you may want to change to fit your custom instance.

Warning: you will see a lot of settings suffixed with PYPATH(S), those
are expecting path(s) to Python callable. In case of a list, the order
matters given that it is a chain of processors.

#### ATTRIBUTION (string or dict)

The attribution of the data that will be used in the API results. Can be a
simple string, or a dict.

    ATTRIBUTION = 'OpenStreetMap Contributors'
    # Or
    ATTRIBUTION = {'source': 'attribution', 'source2': 'attribution2'}

#### BATCH_WORKERS (int)
Number of processes in use when parallelizing tasks such as batch imports or
ngrams computing.

    BATCH_WORKERS = os.cpu_count() - 1

#### DOCUMENT_STORE_PYPATH (Python path)
Python path to a store class for saving documents using another database
engine and save memory.
Check out the dedicated documentation on the [plugins](plugins.md) page.

#### EXTRA_FIELDS (list of dicts)

Sometimes you just want to extend [default fields](#fields-list-of-dicts).

    EXTRA_FIELDS = [
        {'key': 'myfield'},
    ]

#### FIELDS (list of dicts)
The document fields *you want to index*. It's a list of dict, each one defining
an indexed field. Available keys:

- **key** (*mandatory*): the key of the field in the document
- **boost**: optional boost of the field, define how important is the field
  in the index, for example one usually define a greater boost for *name* field
  than for *city* field (default: 1)
- **null**: define if the field can be null (default: True)
- **type**: optional type, can be `name` or `id`, to define NAME_FIELD or ID_FIELD

```
FIELDS = [
    {'key': 'name', 'boost': 4, 'null': False},
    {'key': 'street'},
    {'key': 'postcode',
     'boost': lambda doc: 1.2 if doc.get('type') == 'municipality' else 1},
    {'key': 'city'},
    {'key': 'housenumbers'}
]
```

You can access any fields from your original data source here. For example, `doc.get('type')` refers to the `type` property defined in the BAN json file.

Warning: Indexes are computed during the import. If you already imported data, you need to reset and reimport it after you modified this configuration file.

If you want to control the `id` of the document, for example in order to override documents at reindex, either add a field `_id` in the document,
or define one of the indexed fields with `type: "id"`.

#### FILTERS (list)
A list of fields to be indexed as available filters. Keep in mind that every
filter means bigger index.

    FILTERS = ["type", "postcode"]

#### LICENCE (string or dict)
The licence of the data returned by the API. Can be a simple string, or a dict.

    LICENCE = "ODbL"
    # Or
    LICENCE = {'source': 'licence', 'source2': 'licence2'}

#### LOG_QUERIES (boolean)
Turn this to `True` to log every query received and first result if any. *Note:
only the queries are logged, not any of the other received data.*

    LOG_QUERIES = False

#### LOG_NOT_FOUND (boolean)
Turn this to `True` to log every not found query both through the `search`
endpoint or the `csv` one.

    LOG_NOT_FOUND = False

#### QUERY_MAX_LENGTH (int)
In characters, max accepted length of the query. Prevent huge strings to be
processed.

    QUERY_MAX_LENGTH = 200

#### SLOW_QUERIES (integer)
Define the time (in ms) to log a slow query.

    SLOW_QUERIES = False  # Inactive
    SLOW_QUERIES = 500  # Will log every query longer than 500 ms

#### SYNONYMS_PATHS (list of paths)
Paths to synonym files. Synonyms files are in the format `av, ave => avenue`.

    SYNONYMS_PATHS = ['/path/to/synonyms.txt']

## Advanced settings

Those are internal settings. Change them with caution.

#### BATCH_CHUNK_SIZE (int)
Number of documents to be processed together by each worker during import.

    BATCH_CHUNK_SIZE = 1000

#### BATCH_FILE_LOADER_PYPATH (Python path)
Python path to a callable which will be responsible of loading file on
import and return an iterable.

    BATCH_FILE_LOADER_PYPATH = 'addok.helpers.load_file'

Addok provides three loaders: `load_file`, `load_msgpack_file`
(needs `msgpack-python`) and `load_csv_file`. But you can pass any path to
a loadable function. This function will take a `filepath` as argument, and
should yield dicts.

#### BATCH_PROCESSORS_PYPATHS (iterable of Python paths)
All methods called during the batch process.

    BATCH_PROCESSORS_PYPATHS = [
        'addok.batch.to_json',
        'addok.helpers.index.prepare_housenumbers',
        'addok.ds.store_documents',
        'addok.helpers.index.index_documents',
    ]

#### BUCKET_MIN (int)
The min number of items addok will try to fetch from Redis before scoring and
sorting them. Note that **this is not the number of returned results**.
This may impact performances a lot.

    BUCKET_MIN = 10

#### BUCKET_MAX (int)
The max number of items addok will try to fetch from Redis before scoring and
sorting them. Note that **this is not the number of returned results**.
This may impact performances a lot.

    BUCKET_MAX = 100

#### COMMON_THRESHOLD (int)
Above this threshold, terms are considered commons, and thus with less importance
in the search algorithm.

    COMMON_THRESHOLD = 10000

#### DEFAULT_BOOST (float)
Default score for the relation token to document.

    DEFAULT_BOOST = 1.0

#### DOCUMENT_SERIALIZER_PYPATH (Python path)
Path to the serializer to be used for storing documents. Must have `loads` and
`dumps` methods.

    DOCUMENT_SERIALIZER_PYPATH = 'addok.helpers.serializers.ZlibSerializer'

For a faster option (but using more RAM), use `marshal` instead.

    DOCUMENT_SERIALIZER_PYPATH = 'marshal'

#### GEOHASH_PRECISION (int)
Size of the geohash. The bigger the setting, the smaller the hash.
See [Geohash on Wikipedia](http://en.wikipedia.org/wiki/Geohash).

    GEOHASH_PRECISION = 8

#### IMPORTANCE_WEIGHT (float)
The max inherent score of a document in the final score.

    IMPORTANCE_WEIGHT = 0.1

#### GEO_DISTANCE_WEIGHT (float)
The max inherent score of geographical distance to the provided center (if any) in the final score.

    GEO_DISTANCE_WEIGHT = 0.1

#### INTERSECT_LIMIT (int)
Above this threshold, we avoid intersecting sets.

    INTERSECT_LIMIT = 100000

#### MAX_EDGE_NGRAMS (int)
Maximum length of computed edge ngrams.

    MAX_EDGE_NGRAMS = 20

#### MIN_EDGE_NGRAMS (int)
Minimum length of computed edge ngrams.

    MIN_EDGE_NGRAMS = 3

#### MIN_SCORE (float)
All results with final score below this threshold will not be kept. Score is
between 0 and 1.

    MIN_SCORE = 0.1

#### MAKE_LABELS (func)
Function to override labels built for string comparison with query
at scoring time. Takes a `result` object as argument and must return a
list of strings.

    MAKE_LABELS = lambda r: return [r.name + 'my custom thing']

#### MATCH_THRESHOLD (float between 0 and 1)
Min score used to consider a result may *match* the query.

    MATCH_THRESHOLD = 0.9

#### PROCESSORS_PYPATHS (iterable of Python paths)
Define the various functions to preprocess the text, before indexing and
searching. It's an `iterable` of Python paths. Some functions are built in
(mainly for French at this time, but you can point to any Python function that
is on the pythonpath).

    PROCESSORS_PYPATHS = [
        'addok.helpers.text.tokenize',
        'addok.helpers.text.normalize',
        'addok.helpers.text.flag_housenumber',
        'addok.helpers.text.synonymize',
    ]

#### QUERY_PROCESSORS_PYPATHS (iterable of Python paths)
Additional processors that are run only at query time. By default, only
`check_query_length` is active, it depends on `QUERY_MAX_LENGTH` to avoid DoS.

    QUERY_PROCESSORS_PYPATHS = (
        'addok.helpers.text.check_query_length',
    )

#### RESULTS_COLLECTORS_PYPATHS (iterable of Python paths)
Addok will try each of those in the given order for searching matching results.

    RESULTS_COLLECTORS_PYPATHS = [
        'addok.autocomplete.only_commons_but_geohash_try_autocomplete_collector',
        'addok.helpers.collectors.no_tokens_but_housenumbers_and_geohash',
        'addok.helpers.collectors.no_available_tokens_abort',
        'addok.helpers.collectors.only_commons',
        'addok.autocomplete.no_meaningful_but_common_try_autocomplete_collector',
        'addok.autocomplete.only_commons_try_autocomplete_collector',
        'addok.helpers.collectors.bucket_with_meaningful',
        'addok.helpers.collectors.reduce_with_other_commons',
        'addok.helpers.collectors.ensure_geohash_results_are_included_if_center_is_given',  # noqa
        'addok.fuzzy.fuzzy_collector',
        'addok.autocomplete.autocomplete_meaningful_collector',
        'addok.helpers.collectors.extend_results_extrapoling_relations',
        'addok.helpers.collectors.extend_results_reducing_tokens',
    ]

### SEARCH_RESULT_PROCESSORS_PYPATHS (iterable of Python paths)
Post processing of each result found during search.

    SEARCH_RESULT_PROCESSORS_PYPATHS = [
        'addok.helpers.results.match_housenumber',
        'addok.helpers.results.make_labels',
        'addok.helpers.results.score_by_importance',
        'addok.helpers.results.score_by_autocomplete_distance',
        'addok.helpers.results.score_by_str_distance',
        'addok.helpers.results.score_by_geo_distance',
        'addok.helpers.results.adjust_scores',
    ]
