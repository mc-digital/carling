import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from carling import Exclude, Label, RenameFromTo


class RenameTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"value1": "001", "valueX": "AAA"},
        ]

        expected = [
            {"value1": "001", "valueY": "AAA"},
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | RenameFromTo({"valueX": "valueY"})
            assert_that(actual, equal_to(expected))


class LabelTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"value1": "001", "valueX": "AAA"},
        ]

        expected = [
            {"value1": "001", "valueX": "AAA", "extra1": 1, "extra2": 2},
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | Label(**{"extra1": 1, "extra2": 2})
            assert_that(actual, equal_to(expected))


class ExcludeTest(unittest.TestCase):
    def test(self):

        inputs = [
            {"value1": "001", "valueX": "AAA"},
        ]

        expected = [
            {"value1": "001"},
        ]

        with TestPipeline() as p:
            actual = p | beam.Create(inputs) | Exclude(*["valueX", "valueY"])
            assert_that(actual, equal_to(expected))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    unittest.main()
