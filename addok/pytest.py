import os
import uuid

import pytest


def pytest_configure():
    # Do not import files from the top of the module, otherwise they will
    # not taken into account by the coverage.
    from addok.config import config
    config.__class__.TESTING = True
    # Be sure not to load local config during tests.
    os.environ['ADDOK_CONFIG_MODULE'] = ''
    import logging
    logging.basicConfig(level=logging.DEBUG)
    config.REDIS['db'] = 15
    config.load()


def pytest_runtest_setup(item):
    from addok.db import DB
    assert DB.connection_pool.connection_kwargs['db'] == 15


def pytest_runtest_teardown(item, nextitem):
    from addok.db import DB
    assert DB.connection_pool.connection_kwargs['db'] == 15
    DB.flushdb()


def pytest_addoption(parser):
    group = parser.getgroup("general")
    group._addoption('--shell',
                     action="store_true", dest="addokshell", default=False,
                     help="start addok interactive shell on error.")


def pytest_exception_interact(node, call, report):
    if node.config.getvalue("addokshell"):
        from addok.shell import invoke
        invoke()


class DummyDoc(dict):

    def __init__(self, *args, **kwargs):
        skip_index = kwargs.pop('skip_index', False)
        super().__init__(*args, **kwargs)
        if not skip_index:
            self.index()

    def update(self, **kwargs):
        super().update(kwargs)
        self.index()

    def index(self):
        from addok.batch import process_documents
        process_documents([self.copy()])


@pytest.fixture
def factory(request):
    def _(**kwargs):
        default = {
            'id': uuid.uuid4().hex,
            'type': 'street',
            'name': 'ellington',
            'importance': 0.0,
            'lat': '48.3254',
            'lon': '2.256'
        }
        default.update(kwargs)
        doc = DummyDoc(**default)
        return doc
    return _


@pytest.fixture
def street(factory):
    return factory()


@pytest.fixture
def city(factory):
    return factory(type='city')


@pytest.fixture
def housenumber(factory):
    return factory(housenumbers={'11': {'lat': '48.3254', 'lon': '2.256'}})


@pytest.fixture
def app():
    # Do not import before redis config has been
    # patched.
    from addok.http.wsgi import application
    return application


class MonkeyPatchWrapper(object):
    def __init__(self, monkeypatch, wrapped_object):
        super().__setattr__('monkeypatch', monkeypatch)
        super().__setattr__('wrapped_object', wrapped_object)

    def __getattr__(self, attr):
        return getattr(self.wrapped_object, attr)

    def __setattr__(self, attr, value):
        self.monkeypatch.setattr(self.wrapped_object, attr, value,
                                 raising=False)

    def __delattr__(self, attr):
        self.monkeypatch.delattr(self.wrapped_object, attr)


@pytest.fixture()
def config(request, monkeypatch):
    from addok.config import config as addok_config
    return MonkeyPatchWrapper(monkeypatch, addok_config)
