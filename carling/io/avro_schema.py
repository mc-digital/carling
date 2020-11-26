import json


def load_avro_schema(template_path):
    schema = json.load(open(template_path))

    return schema


def _fill_template_schema(template, var_name, var_values):

    template_fields = template["fields"]
    fields = []

    var_name_in_parentheses = "{" + var_name + "}"

    for f in template_fields:

        if var_name_in_parentheses not in f["name"]:
            fields.append(f)
            continue

        for value in var_values:
            replaced_name = f["name"].format(**{var_name: value})
            fields.append({**f, "name": replaced_name})

    schema = {
        **template,
        "fields": fields,
    }

    return schema


def generate_avro_schema_from_template(template_path, var_name, var_values):
    template = load_avro_schema(template_path)
    return _fill_template_schema(template, var_name, var_values)
