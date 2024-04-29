import json

import pandas as pd
from common.file_utils.FileUtils import FileUtils
from common.marker import this_method_is_only_for_test


class JSONUtils(FileUtils):
    def read_file(self, file_path):
        return pd.read_json(file_path)

    @this_method_is_only_for_test
    def write_file(self, filename, data):
        """ This method is only used by unit tests """
        json_string = json.dumps(data, indent=4)
        with open(filename, 'w') as file:
            file.write(json_string)
