import csv

import pandas as pd
from common.file_utils.FileUtils import FileUtils
from common.marker import this_method_is_only_for_test


class CSVUtils(FileUtils):
    def read_file(self, file_path):
        return pd.read_csv(file_path)

    @this_method_is_only_for_test
    def write_file(self,filename, data):
        """ This method is only used by unit tests """
        keys = []
        for row in data:
            keys.extend(list(row.keys()))
        headers = set(keys)
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for row in data:
                row = {key: row[key] for key in row if key in headers}
                writer.writerow(row)