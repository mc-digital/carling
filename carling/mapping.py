"""
Generic mapping transform utils

Author: Tsuyoki Kumazaki (tsuyoki@mcdigital.jp)
"""

import json

import apache_beam as beam


def Label(**labels):
    """Labels all elements.
    """
    return "Label" >> beam.Map(lambda r: {**r, **labels})


def Select(*keys):
    """Removes all columns which are not specified in `*keys`.
    """
    return "Select" >> beam.Map(lambda r: {k: r[k] for k in keys})


def Project(*keys):
    """Transforms each element into a tuple of values of the specified columns.
    """
    return "Project" >> beam.Map(lambda r: tuple(r[k] for k in keys))


def IndexBy(*keys):
    """Transforms each element `V` into a tuple `(K, V)`.

    `K` is the projection of `V` by `*keys`, which is equal to the tuple
    produced by the `Project` transform.
    """
    return "IndexBy" >> beam.Map(lambda r: (tuple(r[k] for k in keys), r))


def _decimal_default_proc(obj):
    from decimal import Decimal

    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def Stringify():
    """Transforms each element into its JSON representation.
    """

    def s(obj):
        return json.dumps(obj, default=_decimal_default_proc)

    return "Stringify" >> beam.Map(s)


def IndexBySingle(key):
    """Transforms each element `V` into a tuple `(K, V)`.

    The difference between `IndexBySingle(key)` and `IndexBy(key)` with a single
    key is as follows:
    - `IndexBySingle` produces the index as a plain value.
    - `IndexBy` produces the index as a single-element tuple.
    """
    return "IndexBySingle" >> beam.Map(lambda r: (r[key], r))


def RenameFromTo(from_to_key_mapping):
    """Rename columns according to `from_to_key_mapping`.
    """

    def rename(row):
        res = dict(row)
        for k1, k2 in from_to_key_mapping.items():
            if k1 in res:
                v = res.pop(k1)
                res[k2] = v
        return res

    return "Rename" >> beam.Map(rename)


def Exclude(*keys):
    """Removes all columns specified in `*keys`.
    """

    def exclude(row):
        res = dict(row)
        for k in keys:
            if k in res:
                del res[k]
        return res

    return "Exclude" >> beam.Map(exclude)
