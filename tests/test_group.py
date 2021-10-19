import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from carling import (
    DifferencePerKey,
    FilterByKey,
    FilterByKeyUsingSideInput,
    IndexBy,
    Intersection,
    MaxSelectPerKey,
    PartitionRowsContainingNone,
    SingletonOnly,
    UniqueOnly,
)
from carling.test_utils import pprint_equal_to


class UniqueOnlyTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"key": "001", "value": "AAA"},
            {"key": "002", "value": "BBB"},
            {"key": "002", "value": "BBB"},
            {"key": "003", "value": "CCC"},
            {"key": "003", "value": "DDD"},
        ]

        expected = [
            {"key": "001", "value": "AAA"},
            {"key": "002", "value": "BBB"},
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | IndexBy("key") | UniqueOnly()
            assert_that(actual, equal_to(expected))


class SingletonOnlyTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"key": "001", "value": "AAA"},
            {"key": "002", "value": "BBB"},
            {"key": "002", "value": "BBB"},
            {"key": "003", "value": "CCC"},
            {"key": "003", "value": "DDD"},
        ]

        expected = [
            {"key": "001", "value": "AAA"},
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | IndexBy("key") | SingletonOnly()
            assert_that(actual, equal_to(expected))


class IntersectionTest(unittest.TestCase):
    def test(self):

        input_left = [
            (("value1", "AAA"), ("value2", "XXX")),
            (("value1", "BBB"), ("value2", "YYY")),
            (("value1", "001"), ("value2", "001")),
        ]

        input_right = [
            (("value1", "AAA"), ("value2", "XXX")),
            (("value1", "CCC"), ("value2", "ZZZ")),
            (("value1", "001"), ("value2", "002")),
        ]

        expected = [
            (("value1", "AAA"), ("value2", "XXX")),
        ]

        with TestPipeline() as p:
            actual = (
                p | "Create[left]" >> beam.Create(input_left),
                p | "Create[right]" >> beam.Create(input_right),
            ) | Intersection()
            assert_that(actual, equal_to(expected))


class FilterByKeyTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"key": "001", "value": "A"},
            {"key": "002", "value": "A"},
            {"key": "002", "value": "A"},
            {"key": "002", "value": "B"},
            {"key": "003", "value": "A"},
            {"key": "004", "value": "A"},
            {"key": "005", "value": "A"},
        ]

        input_key1 = [
            ("001",),
            ("002",),
            ("003",),
        ]

        input_key2 = [
            ("001",),
            ("002",),
            ("004",),
        ]

        expected = [
            {"key": "001", "value": "A"},
            {"key": "002", "value": "A"},
            {"key": "002", "value": "A"},
            {"key": "002", "value": "B"},
        ]

        with TestPipeline() as p:
            key1 = p | "Create[key1]" >> beam.Create(input_key1)
            key2 = p | "Create[key2]" >> beam.Create(input_key2)
            actual = (
                p
                | "Create[inputs]" >> beam.Create(inputs)
                | IndexBy("key")
                | FilterByKey(key1, key2)
            )
            assert_that(actual, equal_to(expected))


class FilterByKeyUsingSideInputTest(unittest.TestCase):
    def test(self):

        filter_table = [
            {"a": 1, "b": 99991},
            {"a": 2, "b": 99992},
            {"a": 3, "b": 99993},
            {"a": 99994, "b": 4},
        ]

        inputs = [{"a": 1, "b": 1}, {"a": 2, "b": 2}, {"a": 4, "b": 4}]

        a_expected = [{"a": 1, "b": 1}, {"a": 2, "b": 2}]

        b_expected = [{"a": 4, "b": 4}]

        with TestPipeline() as p:
            inputs_coll = p | "create inputs coll" >> beam.Create(inputs)
            filter_coll = p | "create filter coll" >> beam.Create(filter_table)
            a_filtered = inputs_coll | "filter a" >> FilterByKeyUsingSideInput(filter_coll, "a")
            b_filtered = inputs_coll | "filter b" >> FilterByKeyUsingSideInput(filter_coll, "b")
            assert_that(a_filtered, equal_to(a_expected), label="a check")
            assert_that(b_filtered, equal_to(b_expected), label="b check")


class DifferencePerKeyTest(unittest.TestCase):
    def _check(self, left_inputs, right_inputs, expected, keys=["key"], columns=["value"]):

        excluded = ["detail"]

        def filter_extra_info(row):
            error = {k: v for k, v in row["error"].items() if k not in excluded}
            return {**row, "error": error}

        with TestPipeline() as p:
            left = p | "create left" >> beam.Create(left_inputs)
            right = p | "create right" >> beam.Create(right_inputs)
            actual = (left, right) | DifferencePerKey(keys, columns) | beam.Map(filter_extra_info)
            assert_that(actual, pprint_equal_to(expected))

    def test_plain_diff(self):

        left_inputs = [
            {"key": "a", "value": 1},
            {"key": "b", "value": 2},
        ]
        right_inputs = [
            {"key": "a", "value": 1},
            {"key": "b", "value": 3},
        ]
        expected = [
            {
                "key": ("b",),
                "left": left_inputs[1],
                "right": right_inputs[1],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value",
                    "left": 2,
                    "right": 3,
                },
            }
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_type_mismatch(self):

        left_inputs = [{"key": "a", "value": 1}]
        right_inputs = [{"key": "a", "value": 1.0}]
        expected = [
            {
                "key": ("a",),
                "left": left_inputs[0],
                "right": right_inputs[0],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value",
                    "left": "int",
                    "right": "float",
                },
            }
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_array(self):

        left_inputs = [
            {"key": "a", "value": [1, 2, 3]},
            {"key": "b", "value": [1, 2, 3]},
            {"key": "c", "value": [1, 2, 3]},
        ]
        right_inputs = [
            {"key": "a", "value": [1, 2, 3]},
            {"key": "b", "value": [1, 2, 3, 4]},
            {"key": "c", "value": [1, 2, 4]},
        ]
        expected = [
            {
                "key": ("b",),
                "left": left_inputs[1],
                "right": right_inputs[1],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value",
                    "left": 3,
                    "right": 4,
                },
            },
            {
                "key": ("c",),
                "left": left_inputs[2],
                "right": right_inputs[2],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value[2]",
                    "left": 3,
                    "right": 4,
                },
            },
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_dict(self):

        left_inputs = [
            {"key": "a", "value": {"x": "X"}},
            {"key": "b", "value": {"x": "X"}},
            {"key": "c", "value": {"x": "X"}},
        ]
        right_inputs = [
            {"key": "a", "value": {"x": "X"}},
            {"key": "b", "value": {}},
            {"key": "c", "value": {"x": "Y"}},
        ]
        expected = [
            {
                "key": ("b",),
                "left": left_inputs[1],
                "right": right_inputs[1],
                "error": {"reason": "value", "tag": "(root)/value/x"},
            },
            {
                "key": ("c",),
                "left": left_inputs[2],
                "right": right_inputs[2],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value/x",
                    "left": "X",
                    "right": "Y",
                },
            },
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_nested(self):

        left_inputs = [{"key": "a", "value": {"lv1": {"lv2": [{"lv3": [1]}]}}}]
        right_inputs = [{"key": "a", "value": {"lv1": {"lv2": [{"lv3": [2]}]}}}]
        expected = [
            {
                "key": ("a",),
                "left": left_inputs[0],
                "right": right_inputs[0],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value/lv1/lv2[0]/lv3[0]",
                    "left": 1,
                    "right": 2,
                },
            }
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_specified_column_only(self):

        left_inputs = [
            {"key": "a", "value1": 1, "value2": 1},
            {"key": "b", "value1": 2, "value2": 2},
            {"key": "c", "value1": 3, "value2": 3},
        ]
        right_inputs = [
            {"key": "a", "value1": 1, "value2": 7},
            {"key": "b", "value1": 3, "value2": 2},
            {"key": "c", "value1": 3},
        ]
        expected = [
            {
                "key": ("b",),
                "left": left_inputs[1],
                "right": right_inputs[1],
                "error": {
                    "reason": "value",
                    "tag": "(root)/value1",
                    "left": 2,
                    "right": 3,
                },
            }
        ]
        self._check(left_inputs, right_inputs, expected, columns=["value1"])

    def test_missing_value(self):

        left_inputs = [{"key": "a", "value": 1}]
        right_inputs = []
        expected = [
            {
                "key": ("a",),
                "left": left_inputs[0],
                "right": None,
                "error": {"reason": "count", "left": 1, "right": 0},
            }
        ]
        self._check(left_inputs, right_inputs, expected)

    def test_key_conflict(self):

        left_inputs = [
            {"key": "a", "value1": 1, "value2": 1},
            {"key": "a", "value1": 2, "value2": 2},
        ]
        right_inputs = [
            {"key": "a", "value1": 1, "value2": 1},
        ]
        expected = [
            {
                "key": ("a",),
                "left": left_inputs[0],
                "right": right_inputs[0],
                "error": {"reason": "count", "left": 2, "right": 1},
            }
        ]
        self._check(left_inputs, right_inputs, expected, columns=["value1", "value2"])


class MaxSelectByKeyTest(unittest.TestCase):

    inputs = [
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 1, "c": 2},
        {"a": 1, "b": 1, "c": 3},
        {"a": 2, "b": 1, "c": 1},
        {"a": 2, "b": 1, "c": 2},
        {"a": 2, "b": 1, "c": 3},
        {"a": 1, "b": 2, "c": 1},
        {"a": 1, "b": 2, "c": 2},
        {"a": 1, "b": 2, "c": 3},
    ]

    def test_max_select(self):

        expected = [
            {"a": 1, "b": 1, "c": 3},
            {"a": 2, "b": 1, "c": 3},
            {"a": 1, "b": 2, "c": 3},
        ]

        with TestPipeline() as p:
            actual = (
                p
                | "Create Input" >> beam.Create(self.inputs)
                | MaxSelectPerKey(("a", "b"), lambda r: r["c"])
            )
            assert_that(actual, pprint_equal_to(expected))

    def test_max_select_reverse(self):

        expected = [
            {"a": 1, "b": 1, "c": 1},
            {"a": 2, "b": 1, "c": 1},
            {"a": 1, "b": 2, "c": 1},
        ]

        with TestPipeline() as p:
            actual = (
                p
                | "Create Input" >> beam.Create(self.inputs)
                | MaxSelectPerKey(("a", "b"), lambda r: r["c"], reverse=True)
            )
            assert_that(actual, pprint_equal_to(expected))


class PartitionRowsContainingNoneTest(unittest.TestCase):
    def test_null_separation(self):

        inputs = [
            {"a": 1, "b": 1, "c": 1},
            {"a": None, "b": 1, "c": 1},
            {"a": 1, "b": None, "c": 1},
        ]

        expected_default = [
            {"a": 1, "b": 1, "c": 1},
        ]

        expected_contains_none = [
            {"a": None, "b": 1, "c": 1},
            {"a": 1, "b": None, "c": 1},
        ]

        with TestPipeline() as p:
            actual = p | "Create Input" >> beam.Create(inputs) | PartitionRowsContainingNone()

            assert_that(actual[None], pprint_equal_to(expected_default), label="default")
            assert_that(
                actual["contains_none"],
                pprint_equal_to(expected_contains_none),
                label="contains none",
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    unittest.main()
