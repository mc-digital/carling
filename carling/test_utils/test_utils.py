from apache_beam.testing.util import BeamAssertException
import pprint
from deepdiff import DeepDiff


def format_msg(msg_parts):

    return "\n\n".join(
        [
            (
                "------------------------------------\n\n"
                + """{elements_title}:\n\n{pprint_elements}""".format(
                    elements_title=elements_title,
                    pprint_elements=pprint.pformat(elements),
                )
            )
            for (elements_title, elements) in msg_parts
        ]
    )


def pprint_equal_to(expected, deepdiff=False):
    """
    This helper function is based on the "equal_to" function from apache beam,
    but adapted to output results using pretty print.

    Reading the results as a large, unformatted string makes it harder to pick
    out what changed/is missing.
    """

    def _equal(actual):

        expected_list = list(expected)

        # Try to compare actual and expected by sorting. This fails with a
        # TypeError in Python 3 if different types are present in the same
        # collection. It can also raise false negatives for types that don't have
        # a deterministic sort order, like pyarrow Tables as of 0.14.1
        try:
            sorted_expected = sorted(expected)
            sorted_actual = sorted(actual)
            if sorted_expected != sorted_actual:
                raise BeamAssertException(
                    "Failed assert: %r == %r" % (sorted_expected, sorted_actual)
                )
        # Slower method, used in two cases:
        # 1) If sorted expected != actual, use this method to verify the inequality.
        #    This ensures we don't raise any false negatives for types that don't
        #    have a deterministic sort order.
        # 2) As a fallback if we encounter a TypeError in python 3. this method
        #    works on collections that have different types.
        except (BeamAssertException, TypeError):
            unexpected = []
            for element in actual:
                try:
                    expected_list.remove(element)
                except ValueError:
                    unexpected.append(element)
            if unexpected or expected_list:
                msg_parts = []
                msg_parts.append(("Expected", expected))
                msg_parts.append(("Actual", actual))
                if unexpected:
                    msg_parts.append(("Unexpected", unexpected))
                if expected_list:
                    msg_parts.append(("Missing", expected_list))

                if deepdiff:
                    dds = []
                    msg_parts.append(
                        ("DeepDiff (expected / actual)", DeepDiff(expected, actual))
                    )
                raise BeamAssertException(format_msg(msg_parts))

    return _equal
