"""
Generic iter utils
Author: Tsuyoki Kumazaki (tsuyoki@mcdigital.jp)
"""

import itertools


def take_top(iterable):
    return tuple(itertools.islice(iterable, 1))


def is_some(top_tuple):
    return len(top_tuple) > 0


def is_none(top_tuple):
    return len(top_tuple) == 0


def unwrap(top_tuple):
    assert len(top_tuple) > 0
    return top_tuple[0]


def unwrap_or_none(top_tuple):
    if len(top_tuple) > 0:
        return top_tuple[0]
    return None


def take_as_singleton(iterable):
    iterable = iter(iterable)
    v = take_top(iterable)
    if is_none(v):
        return tuple()
    w = take_top(iterable)
    if is_some(w):
        return tuple()
    return v
