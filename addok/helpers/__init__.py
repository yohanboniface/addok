from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from importlib import import_module
from math import asin, cos, exp, radians, sin, sqrt
import os

from progressist import ProgressBar


def iter_pipe(pipe, processors):
    """Allow for iterators to return either an item or an iterator of items."""
    if isinstance(pipe, str):
        pipe = [pipe]
    for it in processors:
        pipe = it(pipe)
    yield from pipe


def import_by_path(path):
    """
    Import functions or class by their path. Should be of the form:
    path.to.module.func
    """
    if not isinstance(path, str):
        return path
    module_path, name = path.rsplit('.', 1)
    module = import_module(module_path)
    attr = getattr(module, name)
    return attr


def yielder(func):
    @wraps(func)
    def wrapper(pipe):
        for item in pipe:
            yield func(item)
    return wrapper


def haversine_distance(point1, point2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).
    """
    lat1, lon1 = point1
    lat2, lon2 = point2

    # Convert decimal degrees to radians.
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula.
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth.
    km = 6367 * c
    return km


def km_to_score(km):
    # Score between 0 and 0.1 (close to 0 km will be close to 0.1, and 100 and
    # above will be 0).
    return 0.0 if km > 100 else .1 * exp(-(km / 50.0) ** 2)


COLORS = {
    'red': '31',
    'green': '32',
    'yellow': '33',
    'blue': '34',
    'magenta': '35',
    'cyan': '36',
    'white': '37',
    'reset': '39'
}


def colorText(s, color):
    # color should be a string from COLORS
    return '\033[%sm%s\033[%sm' % (COLORS[color], s, COLORS['reset'])


def red(s):
    return colorText(s, 'red')


def green(s):
    return colorText(s, 'green')


def yellow(s):
    return colorText(s, 'yellow')


def blue(s):
    return colorText(s, 'blue')


def magenta(s):
    return colorText(s, 'magenta')


def cyan(s):
    return colorText(s, 'cyan')


def white(s):
    return colorText(s, 'white')


class Bar(ProgressBar):
    animation = '{spinner}'
    template = '{prefix} {animation} Done: {done} | Elapsed: {elapsed}'


def parallelize(func, iterable, chunk_size=10000, **bar_kwargs):
    bar = Bar(**bar_kwargs)

    def chunkize():
        chunk = []
        count = 0
        for i in iterable:
            count += 1
            chunk.append(i)
            if count % chunk_size == 0:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    with ProcessPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        bar(step=0)
        for r in executor.map(func, chunkize()):
            bar(step=len(r))
        bar.finish()
