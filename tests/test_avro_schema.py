import logging
import unittest

from carling.io.avro_schema import _fill_template_schema


class TemplateSchemaTest(unittest.TestCase):
    def test(self):
        template_schema = {
            "name": "root",
            "type": "records",
            "fields": [
                {"name": "XXX", "type": "int"},
                {"name": "YYY_{num}", "type": "float"},
            ],
        }

        var_name = "num"
        var_values = [1, 2, 3]

        expected = {
            "name": "root",
            "type": "records",
            "fields": [
                {"name": "XXX", "type": "int"},
                {"name": "YYY_1", "type": "float"},
                {"name": "YYY_2", "type": "float"},
                {"name": "YYY_3", "type": "float"},
            ],
        }

        actual = _fill_template_schema(template_schema, var_name, var_values)
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    unittest.main()
