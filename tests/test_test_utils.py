import pprint
import unittest

from apache_beam.testing.util import BeamAssertException

from carling.test_utils import pprint_equal_to

CORRECT_VALUE = [{"value": [1, 2, 3]}, {"value": [4, 5, 6]}]
ERR_VALUE = [{"value": [7, 8, 9]}, {"value": [10, 11, 12]}]

EXPECTED_PPRINT_MESSAGE = """------------------------------------

Expected:

{pprint_correct}

------------------------------------

Actual:

{pprint_err}

------------------------------------

Unexpected:

{pprint_err}

------------------------------------

Missing:

{pprint_correct}""".format(
    pprint_correct=pprint.pformat(CORRECT_VALUE), pprint_err=pprint.pformat(ERR_VALUE)
)


class TestPprintEqualTo(unittest.TestCase):
    def test_passthrough(self):
        testfn = pprint_equal_to(CORRECT_VALUE)
        testfn(CORRECT_VALUE)

    def test_error_message(self):
        testfn = pprint_equal_to(CORRECT_VALUE)
        try:
            testfn(ERR_VALUE)

        except BeamAssertException as bae:
            self.maxDiff = None
            self.assertEqual(str(bae), EXPECTED_PPRINT_MESSAGE)


DEEP_DIFF_EXPECTED_MESSAGE = """\n\n------------------------------------

DeepDiff (expected / actual):

{'values_changed': {"root[0]['value'][0]": {'new_value': 7, 'old_value': 1},
                    "root[0]['value'][1]": {'new_value': 8, 'old_value': 2},
                    "root[0]['value'][2]": {'new_value': 9, 'old_value': 3},
                    "root[1]['value'][0]": {'new_value': 10, 'old_value': 4},
                    "root[1]['value'][1]": {'new_value': 11, 'old_value': 5},
                    "root[1]['value'][2]": {'new_value': 12, 'old_value': 6}}}"""


class TestPprintEqualToDeepDiff(unittest.TestCase):
    def test_passthrough(self):
        testfn = pprint_equal_to(CORRECT_VALUE)
        testfn(CORRECT_VALUE)

    def test_error_message(self):
        testfn = pprint_equal_to(CORRECT_VALUE, deepdiff=True)
        try:
            testfn(ERR_VALUE)

        except BeamAssertException as bae:
            self.maxDiff = None
            self.assertEqual(str(bae), EXPECTED_PPRINT_MESSAGE + DEEP_DIFF_EXPECTED_MESSAGE)
