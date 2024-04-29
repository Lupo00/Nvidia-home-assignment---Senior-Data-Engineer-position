import datetime
import random
import string

import yaml

from common.file_utils.FileUtilsFactory import FileUtilsFactory


class SchemaElement:
    def __init__(self, type: str, size: int, name: [str], mandatory: bool = False):
        self.type = type
        self.size = size
        self.name = name
        self.mandatory = mandatory

    def get_size(self):
        return self.size

    def get_type(self):
        return self.type

    def get_name(self):
        if "Test Date" in self.name:
            return "test_date"
        return self.name[random.randint(0, len(self.name) - 1)]

    def is_mandatory(self):
        return self.mandatory


def generate_file(schema: [SchemaElement], file_size: int, filename_path: str, filename: str):
    rows = []
    row_failures = 0
    for i in range(file_size):
        row = {}
        is_row_failed = False
        for schema_element in schema:
            if random.uniform(0, 1) < 0.999:
                name = schema_element.get_name()
                row[name] = generate_type(schema_element.get_type(), schema_element.get_size())
                if (schema_element.get_type() != 'int' and (
                        len(row[name]) > schema_element.get_size())) and schema_element.is_mandatory():
                    is_row_failed = True
            else:
                if schema_element.is_mandatory():
                    is_row_failed = True

        if is_row_failed:
            row_failures = row_failures + 1
        rows.append(row)
    FileUtilsFactory.create_utils(filename).write_file(f'{filename_path}/source/{filename}', rows)
    with open(f"{filename_path}/failures_counter/{filename.split('.')[0].split('/')[-1]}.txt", "w") as file:
        # Write the value to the file
        file.write(str(row_failures))


def generate_type(type: str, size: int):
    if type == "str":
        return generate_random_string(random.randint(1, size + 1))
    if type == "date":
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if type == "int":
        return random.randint(1, 100)
    raise Exception("not implemented yet")


def generate_random_string(length: int):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


def run(min_rows: int = 5, max_rows: int = 10, amount_of_files: int = 20,
        filename_path: str = "test_files/test3_files/"):
    types = ["json", "xml", "csv"]
    with open('../config/schema.yaml', 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    for i in range(amount_of_files):
        schema = [SchemaElement(config[config_element]['type'],
                                config[config_element]['size'] if 'size' in config[
                                    config_element] else 100,
                                mandatory=config[config_element]['mandatory'] if 'mandatory' in config[
                                    config_element] else False,
                                name=config[config_element]['supported'] + [config_element] if "supported" in config[
                                    config_element] else [config_element])
                  for config_element in config if config_element != 'source_type' and config_element != "source_file"]

        file_name = f"itest_example_{i}.{types[i % 3]}"
        generate_file(schema, int(random.uniform(min_rows, max_rows)), filename_path, file_name)


if __name__ == '__main__':
    run()
