# Carling

[![CI](https://github.com/mc-digital/carling/actions/workflows/ci.yml/badge.svg)](https://github.com/mc-digital/carling/actions?query=workflow%3ACI)
[![versions](https://img.shields.io/pypi/pyversions/carling.svg)](https://pypi.org/project/carling/)
[![pypi](https://img.shields.io/pypi/v/carling)](https://pypi.org/project/carling/)
[![license](https://img.shields.io/pypi/l/carling)](https://github.com/mc-digital/carling/blob/main/LICENSE)

Via [Wikipedia](<https://en.wikipedia.org/wiki/Carling_(sailing)>):

> Carlings are pieces of timber laid fore and aft under the deck of a ship, from one beam to another.
> They serve as a foundation for the whole body of the ship.

Useful transforms for supporting our apache beam pipelines.

## Mapping transform utils

#### `carling.Label`

Labels all elements.

#### `carling.Select`

Removes all columns which are not specified in `*keys`.

#### `carling.Project`

Transforms each element into a tuple of values of the specified columns.

#### `carling.IndexBy`

Transforms each element `V` into a tuple `(K, V)`.

`K` is the projection of `V` by `*keys`, which is equal to the tuple
produced by the `Project` transform.

#### `carling.Stringify`

Transforms each element into its JSON representation.

#### `carling.IndexBySingle`

Transforms each element `V` into a tuple `(K, V)`.

The difference between `IndexBySingle(key)` and `IndexBy(key)` with a single
key is as follows:

- `IndexBySingle` produces the index as a plain value.
- `IndexBy` produces the index as a single-element tuple.

#### `carling.RenameFromTo`

Rename columns according to `from_to_key_mapping`.

#### `carling.Exclude`

Removes all columns specified in `*keys`.

## Grouping transform utils

Generic grouping transform utils

#### `carling.UniqueOnly`

Produces elements that are the only elements per key after deduplication.

Given a `PCollection` of `(K, V)`,
this transform produces the collection of all `V`s that do not share
the same corresponding `K`s with any other elements after deduplicating
all equivalent `(K, V)` pairs.

This transform is equivalent to `SingletonOnly` with `apache_beam.Distinct`.

`[(1, "A"), (2, "B1"), (2, "B2"), (3, "C"), (3, "C"), (4, "A")]` will be
transformed into `["A", "C", "A"]`.

#### `carling.SingletonOnly`

Produces elements that are the only elements per key.

Given a `PCollection` of `(K, V)`,
this transform produces the collection of all `V`s that do not share
the same corresponding `K`s with any other elements.

`[(1, "A"), (2, "B1"), (2, "B2"), (3, "C"), (3, "C"), (4, "A")]` will be
transformed into `["A", "A"]`.

#### `carling.Intersection`

Produces the intersection of given `PCollection`s.

Given a list of `PCollection`s,
this transform produces every element that appears in all collections of
the list.
Elements are deduplicated before taking the intersection.

#### `carling.FilterByKey`

Filters elements by their keys.

The constructor receives one or more `PCollection`s of `K`s,
which are regarded as key lists.
Given a `PCollection` of `(K, V)`,
this transform discards all elements with `K`s that do not appear
in the key lists.

If multiple collections are given to the constructor,
this transform treats the intersection of them as the key list.

#### `carling.FilterByKeyUsingSideInput`

Filters a single collection by a single lookup collection, using a common key.

Given: - a `PCollection` (lookup_entries) of `(V)`, as a lookup collection - a `PCollection` (pcoll) of `(V)`, as values to be filtered - a common key (filter_key)

A dictionary called `filter_dict` - is created by mapping the value of `filter_key`
for each entry in `lookup_entries` to True.

Then, for each item in pcoll, the value associated with `filter_key` checkd against
`filter_dict`, and if it is found, the entry passes through. Otherwise, the entry is
discarded.

Note: `lookup_entries` will be used as a **side input**, so care
must be taken regarding the size of the `lookup_entries`

#### `carling.DifferencePerKey`

Produces the difference per key between two `PCollection`s.

Given two `PCollection`s of `V`,
this transform indexes the collections by the specified keys `primary_keys`,
compares corresponding two `V` lists for every `K`,
and produces the difference per `K`.
If there is no difference, this transform produces nothing.

Two `V` lists are considered to be different if the numbers of elements
differ or two elements of the lists with a same index differ
at one of the specified columns `columns`.

#### `carling.SortedSelectPerKey`

- Groups items by a set of `keys` -- column names per row
- Emits the "MAX" _value_ for each collection as defined by the `key_fn`
- Can emit "MIN" by passing `reverse=True` kwarg

#### `carling.PartitionRowsContainingNone`

Emits two tagged PCollections:

- Default (`result[None]`): Rows are guaranteed not to have any `None` values
- `result["contains_none"]`: Rows for which at least one column had a `None` value

## Categorical

#### `carling.CreateCategoricalDicts`

For a set of columnular data inputs this function takes:

    - cat_cols:

        Type: `[str]`

        An array of "categorical" columns

    - existing_dicts:

        Type: `PCollection[(string, string, int)]`

        Rows of tuples of type:
        (column, previously_seen_value, mapped_unique_int)

        Mapping a set of "previously seen" keys to unique int values for each
        column.
        Not optional.
        If none exist, pass an empty PCollection.

It then creates a transform which takes a pcollection and

    - looks at the input pcoll for unseen values in each categorical column
    - creates new unique integers for each distinct unseen value, starting at max(previous value for column)+1
    - ammends the existing mappings with (col, unseen_value, new_unique_int)

Output is:

    - Type: `PCollection[(string, string, int)]`

This is useful for preparing data to be trained by eg. LightGBM

#### `carling.ReplaceCategoricalColumns`

- Utilizes the "categorical dictionary rows" generated by `CreateCategoricalDicts` which is a list of pairs of type of `(column, value,unique_int)`.

- Replaces each column with the appropriate value found in the mapping.

## Test Utils

#### `carling.test_utils.pprint_equal_to`

This module contains the `equal_to` function from apache beam, but adapted to output results using pretty print. Reading the results as a large, unformatted string makes it harder to pick out what changed/is missing.

## General Util

#### `carling.LogSample`

Print items of the given `PCollection` to the log.

`LogSample` prints the JSON representations of the input items to the Python's
standard logging system.

To avoid too much log entries being printed, `LogSample` limits the number of
logged items. The constructor parameter `n` determines the limit.

By default, `LogSample` prints logs with the `INFO` log level. The constructor
parameter `level` determines the level.

#### `carling.ReifyMultiValueOption`

Prepares multi-value delimited options. Useful in contexts where
you want to create a multi-value option in a template environment.

- inputs:
  - delimited string option
  - optional delimiter string (default is "|")

* output:
  - Type: `PCollection[str]`
