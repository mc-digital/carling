"""
Generic grouping transform utils

Author: Tsuyoki Kumazaki (tsuyoki@mcdigital.jp)
"""
from functools import reduce

import apache_beam as beam

from carling.mapping import IndexBy
from carling.iter_utils import (
    take_top,
    is_none,
    is_some,
    unwrap,
    unwrap_or_none,
    take_as_singleton,
)


def _merge_two(x, y):
    xv, xn = x
    yv, yn = y
    if xn == 0:
        return y
    if yn == 0:
        return x
    if xv == yv:
        return (xv, xn + yn)
    return ([], xn + yn)


class _UniqueKeyCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return ([], 0)

    def add_input(self, acc, row):
        return _merge_two(acc, ([row], 1))

    def merge_accumulators(self, accs):
        return reduce(_merge_two, accs)

    def extract_output(self, acc):
        return acc[0]


class UniqueKeyCombine(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.CombinePerKey(_UniqueKeyCombineFn())


class UniqueOnly(beam.PTransform):

    """Produces elements that are the only elements per key after deduplication.

    Given a `PCollection` of `(K, V)`,
    this transform produces the collection of all `V`s that do not share
    the same corresponding `K`s with any other elements after deduplicating
    all equivalent `(K, V)` pairs.

    This transform is equivalent to `SingletonOnly` with `apache_beam.Distinct`.

    `[(1, "A"), (2, "B1"), (2, "B2"), (3, "C"), (3, "C"), (4, "A")]` will be
    transformed into `["A", "C", "A"]`.
    """

    def expand(self, pcoll):
        # As the input collection may include `None`s, this transform uses
        # lists, instead of `None`s, to express the emptyness of values.
        return (
            pcoll
            | "Remove Non-unique Elements" >> UniqueKeyCombine()
            | "Remove None" >> beam.Filter(lambda kv: len(kv[1]) > 0)
            | "Unwrap Values" >> beam.Map(lambda kv: kv[1][0])
        )


class SingletonOnly(beam.PTransform):

    """Produces elements that are the only elements per key.

    Given a `PCollection` of `(K, V)`,
    this transform produces the collection of all `V`s that do not share
    the same corresponding `K`s with any other elements.

    `[(1, "A"), (2, "B1"), (2, "B2"), (3, "C"), (3, "C"), (4, "A")]` will be
    transformed into `["A", "A"]`.
    """

    def expand(self, pcoll):
        # As the input collection may include `None`s, this transform uses
        # tuples, instead of `None`s, to express the emptyness of values.
        return (
            pcoll
            | "Group" >> beam.GroupByKey()
            | "Remove Non-singleton Elements"
            >> beam.Map(lambda kv: take_as_singleton(kv[1]))
            | "Remove None" >> beam.Filter(lambda v: len(v) > 0)
            | "Unwrap Values" >> beam.Map(lambda v: v[0])
        )


class _IntersectionDoFn(beam.DoFn):
    def process(self, row):

        key, iter_list = row
        iter_list = list(iter_list)

        for iterable in iter_list:
            maybe_top = take_top(iterable)
            if is_none(maybe_top):
                return

        yield key


class Intersection(beam.PTransform):

    """Produces the intersection of given `PCollection`s.

    Given a list of `PCollection`s,
    this transform produces every element that appears in all collections of
    the list.
    Elements are deduplicated before taking the intersection.
    """

    def expand(self, pcoll_list):
        pcoll_list = list(pcoll_list)
        keyed_pcolls = (
            pcoll | f"Map[{num}]" >> beam.Map(lambda value: (value, None))
            for num, pcoll in enumerate(pcoll_list)
        )
        return (
            keyed_pcolls
            | "Group" >> beam.CoGroupByKey()
            | "Extract" >> beam.ParDo(_IntersectionDoFn())
        )


class _FilterByKeyDoFn(beam.DoFn):
    def process(self, row):

        _, value_list = row
        value_list = list(value_list)
        head_iter, tail_iters = value_list[0], value_list[1:]

        for it in tail_iters:
            if is_none(take_top(it)):
                return
        for v in head_iter:
            yield v


class FilterByKey(beam.PTransform):

    """Filters elements by their keys.

    The constructor receives one or more `PCollection`s of `K`s,
    which are regarded as key lists.
    Given a `PCollection` of `(K, V)`,
    this transform discards all elements with `K`s that do not appear
    in the key lists.

    If multiple collections are given to the constructor,
    this transform treats the intersection of them as the key list.
    """

    def __init__(self, *key_pcolls):
        super().__init__()
        self._key_pcolls = key_pcolls

    def expand(self, pcoll):
        keys = (
            key_pcoll | f"Map[{num}]" >> beam.Map(lambda key: (key, None))
            for num, key_pcoll in enumerate(self._key_pcolls)
        )
        return (pcoll, *keys) | beam.CoGroupByKey() | beam.ParDo(_FilterByKeyDoFn())


@beam.ptransform_fn
def FilterByKeyUsingSideInput(pcoll, lookup_entries, filter_key):
    """
    Filters a single collection by a single lookup collection, using a common key.

    Given:
      - a `PCollection` (lookup_entries) of `(V)`, as a lookup collection
      - a `PCollection` (pcoll) of `(V)`, as values to be filtered
      - a common key (filter_key)

    A dictionary called `filter_dict` - is created by mapping the value of `filter_key`
    for each entry in `lookup_entries` to True.

    Then, for each item in pcoll, the value associated with `filter_key` checkd against
    `filter_dict`, and if it is found, the entry passes through. Otherwise, the entry is
    discarded.

    Note: `lookup_entries` will be used as a **side input**, so care
    must be taken regarding the size of the `lookup_entries`

    """

    filter_dict_prepared = beam.pvalue.AsDict(
        lookup_entries | beam.Map(lambda row: (row[filter_key], True))
    )

    def _filter_fn(row, filter_dict):
        return row[filter_key] in filter_dict

    return pcoll | beam.Filter(_filter_fn, filter_dict=filter_dict_prepared)


def _compare(left, right, tag, dict_keys=None):
    def tyname(obj):
        return type(obj).__name__

    if type(left) != type(right):
        return {
            "tag": tag,
            "detail": "type mismatch",
            "left": tyname(left),
            "right": tyname(right),
        }

    if isinstance(left, list):

        if len(left) != len(right):
            return {
                "tag": tag,
                "detail": "different array length",
                "left": len(left),
                "right": len(right),
            }
        for index, (lv, rv) in enumerate(zip(left, right)):
            diff = _compare(lv, rv, f"{tag}[{index}]")
            if diff is not None:
                return diff

    elif isinstance(left, dict):

        keys = set(left.keys()) | set(right.keys())

        if dict_keys is not None:
            keys = keys & set(dict_keys)

        for k in keys:
            next_tag = f"{tag}/{k}"
            if k not in left:
                return {"tag": next_tag, "detail": "does not exist in left"}
            if k not in right:
                return {"tag": next_tag, "detail": "does not exist in right"}
            diff = _compare(left[k], right[k], next_tag)
            if diff is not None:
                return diff
    else:

        if left != right:
            return {
                "tag": tag,
                "detail": "values not equal",
                "left": left,
                "right": right,
            }

    return None


class _DifferencePerKeyDoFn(beam.DoFn):
    def __init__(self, columns):
        super().__init__()
        self._columns = columns

    def process(self, row):

        key, value_dict = row
        left_iter = iter(value_dict["left"])
        right_iter = iter(value_dict["right"])

        l1 = take_top(left_iter)
        l2 = take_top(left_iter)
        r1 = take_top(right_iter)
        r2 = take_top(right_iter)

        l_count = [is_some(v) for v in [l1, l2]].count(True)
        r_count = [is_some(v) for v in [r1, r2]].count(True)

        error = None
        if l_count != 1 or r_count != 1:
            error = {
                "reason": "count",
                "left": l_count,
                "right": r_count,
            }
        else:
            lv = unwrap_or_none(l1)
            rv = unwrap_or_none(r1)
            result = _compare(lv, rv, "(root)", dict_keys=self._columns)
            if result is not None:
                error = {"reason": "value", **result}

        if error is not None:
            yield {
                "key": key,
                "error": error,
                "left": unwrap_or_none(l1),
                "right": unwrap_or_none(r1),
            }


class DifferencePerKey(beam.PTransform):

    """Produces the difference per key between two `PCollection`s.

    Given two `PCollection`s of `V`,
    this transform indexes the collections by the specified keys `primary_keys`,
    compares corresponding two `V` lists for every `K`,
    and produces the difference per `K`.
    If there is no difference, this transform produces nothing.

    Two `V` lists are considered to be different if the numbers of elements
    differ or two elements of the lists with a same index differ
    at one of the specified columns `columns`.
    """

    def __init__(self, primary_keys, columns):
        super().__init__()
        self._primary_keys = primary_keys
        self._columns = columns

    def expand(self, pcolls):
        pcolls = list(pcolls)
        assert len(pcolls) == 2
        left, right = pcolls[0], pcolls[1]
        return (
            {
                "left": left | "Index[left]" >> IndexBy(*self._primary_keys),
                "right": right | "Index[right]" >> IndexBy(*self._primary_keys),
            }
            | beam.CoGroupByKey()
            | beam.ParDo(_DifferencePerKeyDoFn(self._columns))
        )


@beam.ptransform_fn
def MaxSelectPerKey(pcoll, index_keys, sort_key_fn, reverse=False):
    """
    - Groups items by key
    - Sorts using a key function
    - Emits the "MAX" _value_ for each collection - key is stripped.
    - Can emit "MIN" by passing reverse=True kwarg
    """
    return (
        pcoll
        | f"Index by {index_keys}" >> IndexBy(*index_keys)
        | f"Top 1 per key"
        >> beam.combiners.Top.PerKey(1, key=sort_key_fn, reverse=reverse)
        | "De-Index" >> beam.Map(lambda k_v: k_v[1][0])
    )


@beam.ptransform_fn
def PartitionRowsContainingNone(pcoll):
    """
    Emits two tagged pcollections:

      - None: Default emitted collection.
              Rows are guaranteed not to have any `None` values
      - contains_none: At least one column in the row had a `None` value
    """

    def _separator(row):
        if any([value is None for value in row.values()]):
            yield beam.pvalue.TaggedOutput("contains_none", row)
        else:
            yield row

    return pcoll | beam.ParDo(_separator).with_outputs()
