import os
import imp
import importlib
import sys

from addok import hooks
from . import default


class Config(dict):

    TESTING = False

    def __init__(self):
        self._post_load_func = []
        self.loaded = False
        self.path_keys = [
            'QUERY_PROCESSORS', 'RESULTS_COLLECTORS',
            'SEARCH_RESULT_PROCESSORS', 'REVERSE_RESULT_PROCESSORS',
            'PROCESSORS', 'INDEXERS', 'DEINDEXERS', 'BATCH_PROCESSORS',
            'SEARCH_PREPROCESSORS', 'RESULTS_FORMATTERS',
            'HOUSENUMBER_PROCESSORS', 'DOCUMENT_PROCESSORS',
        ]
        self.plugins = [
            'addok.shell',
            'addok.http.base',
            'addok.batch',
            'addok.pairs',
            'addok.fuzzy',
            'addok.autocomplete',
        ]
        super().__init__()
        self.extend_from_object(default)

    def load(self):
        if self.loaded:
            return
        self.loaded = True
        self.load_core_plugins()
        if not Config.TESTING:
            # We don't want to autoload installed plugin during tests.
            hooks.load()
        hooks.preconfigure(self)
        self.load_local()
        hooks.configure(self)
        self.resolve()
        self.post_process()
        print('Addok loaded plugins: {}'.format(
                                             ', '.join(hooks.plugins.keys())))

    def on_load(self, func):
        self._post_load_func.append(func)
        return func

    def extend_from_object(self, obj):
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)

    def load_core_plugins(self):
        for path in self.plugins:
            plugin = importlib.import_module(path)
            hooks.register(plugin)

    def load_local(self):
        path = (os.environ.get('ADDOK_CONFIG_MODULE')
                or os.path.join('/etc', 'addok', 'addok.conf'))
        if not os.path.exists(path):
            print('No local config file found in "{}".'.format(path))
            return

        d = imp.new_module('config')
        d.__file__ = path
        try:
            with open(path) as config_file:
                exec(compile(config_file.read(), path, 'exec'), d.__dict__)
        except IOError as e:
            from addok.helpers import red
            print(red('Unable to import {} from '
                      'ADDOK_CONFIG_MODULE'.format(path)))
            sys.exit(e)
        else:
            print('Loaded local config from', path)
            self.extend_from_object(d)

    def __getattr__(self, key):
        return self.get(key)

    def __setattr__(self, key, value):
        self[key] = value

    def post_process(self):
        self.FIELDS.extend(self.EXTRA_FIELDS)
        for field in self.FIELDS:
            key = field['key']
            if (field.get('type') == 'housenumbers' or
                    key == 'housenumbers'):
                self.HOUSENUMBERS_FIELD = key
                field['type'] = 'housenumbers'
            elif field.get('type') == 'name' or key == 'name':
                self.NAME_FIELD = key
                field['type'] = 'name'
        for func in self._post_load_func:
            func()

    def resolve(self):
        for key in self.path_keys:
            self.resolve_paths(key)

    def resolve_paths(self, key):
        from addok.helpers import import_by_path
        attr = self[key]
        for idx, path in enumerate(attr):
            attr[idx] = import_by_path(path)


config = Config()
