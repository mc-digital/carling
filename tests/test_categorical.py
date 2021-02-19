import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from carling.test_utils import pprint_equal_to

from carling import (
    PairWithIndexNumber,
    DigestCategoricalColumns,
    CreateCategoricalDicts,
    ReplaceCategoricalColumns,
)


class PairWithIndexNumberTest(unittest.TestCase):
    def test(self):

        inputs = [
            "AAA",
            "BBB",
            "CCC",
            "AAA",
            "EEE",
        ]

        # FIXME: Order of the indices can be arbitrary.
        #        example: AAA -> 3, BBB -> 2, CCC -> 1, EEE -> 0
        #        This test assumes specific order.
        expected = [
            ("AAA", 0),
            ("BBB", 1),
            ("CCC", 2),
            ("EEE", 3),
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | PairWithIndexNumber()
            assert_that(actual, equal_to(expected))


class DigestCategoricalColumnsTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"key": 1, "value1": "A", "value2": "X"},
            {"key": 2, "value1": "A", "value2": "Y"},
            {"key": 3, "value1": "B", "value2": "X"},
            {"key": 4, "value1": "B", "value2": "Y"},
        ]
        # FIXME: Order of the indices can be arbitrary.
        #        example: A -> 1, B -> 0, X -> 1, Y -> 0
        #        This test assumes specific order.
        expected = [
            {"key": 1, "value1": 0, "value2": 0},
            {"key": 2, "value1": 0, "value2": 1},
            {"key": 3, "value1": 1, "value2": 0},
            {"key": 4, "value1": 1, "value2": 1},
        ]
        cat_cols = ["value1", "value2"]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | DigestCategoricalColumns(cat_cols)
            assert_that(actual, pprint_equal_to(expected, deepdiff=True))


inputs_raw = [
    # Seen values
    {"key": 1, "column1": "A", "column2": "X", "column3": "L"},
    # Unseen values
    {"key": 2, "column1": "B", "column2": "Y", "column3": "M"},
    # Mixed
    {"key": 3, "column1": "C", "column2": "Z", "column3": "N"},
    # Repeat Row
    {"key": 4, "column1": "C", "column2": "Z", "column3": "N"},
]

cat_cols = ["column1", "column2", "column3"]

existing_dict_rows_raw = [
    ("column1", "A", 10),
    ("column1", "NOT IN INPUT", 11),
    ("column2", "X", 2),
    ("column2", "Z", 3),
]


class ReplaceCategoricalColumnsTest(unittest.TestCase):
    def test_use(self):

        expected = [
            # Seen values
            {"key": 1, "column1": 10, "column2": 2, "column3": None},
            # Unseen values
            {"key": 2, "column1": None, "column2": None, "column3": None},
            # Mixed
            {"key": 3, "column1": None, "column2": 3, "column3": None},
            # Repeat Row
            {"key": 4, "column1": None, "column2": 3, "column3": None},
        ]

        with TestPipeline() as p:
            existing_dict_rows = p | "create existing dicts" >> beam.Create(
                existing_dict_rows_raw
            )

            inputs = p | "create inputs" >> beam.Create(inputs_raw)

            actual = inputs | ReplaceCategoricalColumns(cat_cols, existing_dict_rows)

            assert_that(actual, pprint_equal_to(expected, deepdiff=True))

    def test_use_with_default_unseen(self):

        expected = [
            # Seen values
            {"key": 1, "column1": 10, "column2": 2, "column3": 0},
            # Unseen values
            {"key": 2, "column1": 0, "column2": 0, "column3": 0},
            # Mixed
            {"key": 3, "column1": 0, "column2": 3, "column3": 0},
            # Repeat Row
            {"key": 4, "column1": 0, "column2": 3, "column3": 0},
        ]

        with TestPipeline() as p:
            existing_dict_rows = p | "create existing dicts" >> beam.Create(
                existing_dict_rows_raw
            )

            inputs = p | "create inputs" >> beam.Create(inputs_raw)

            actual = inputs | ReplaceCategoricalColumns(
                cat_cols, existing_dict_rows, default_unseen=0
            )

            assert_that(actual, pprint_equal_to(expected, deepdiff=True))


class DigestCategoricalColumnsExistingTest(unittest.TestCase):
    def test_creation(self):

        expected = [
            # Column 1
            ("column1", "A", 10),
            ("column1", "NOT IN INPUT", 11),
            ("column1", "B", 12),
            ("column1", "C", 13),
            # Column 2
            ("column2", "X", 2),
            ("column2", "Z", 3),
            ("column2", "Y", 4),
            # Column 3
            ("column3", "L", 1),
            ("column3", "M", 2),
            ("column3", "N", 3),
        ]

        with TestPipeline() as p:
            existing_dict_rows = p | "create existing dicts" >> beam.Create(
                existing_dict_rows_raw
            )

            inputs = p | "create inputs" >> beam.Create(inputs_raw)

            categorical_dicts = inputs | CreateCategoricalDicts(
                cat_cols, existing_dict_rows
            )

            assert_that(
                categorical_dicts,
                pprint_equal_to(expected),
            )

    def test_use(self):

        expected = [
            # Seen values
            {"key": 1, "column1": 10, "column2": 2, "column3": 1},
            # Unseen values
            {"key": 2, "column1": 12, "column2": 4, "column3": 2},
            # Mixed
            {"key": 3, "column1": 13, "column2": 3, "column3": 3},
            # Repeat Row
            {"key": 4, "column1": 13, "column2": 3, "column3": 3},
        ]

        with TestPipeline() as p:
            existing_dict_rows = p | "create existing dicts" >> beam.Create(
                existing_dict_rows_raw
            )

            inputs = p | "create inputs" >> beam.Create(inputs_raw)

            categorical_dicts = inputs | CreateCategoricalDicts(
                cat_cols, existing_dict_rows
            )

            actual = inputs | ReplaceCategoricalColumns(cat_cols, categorical_dicts)

            assert_that(
                actual,
                pprint_equal_to(expected),
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    unittest.main()
