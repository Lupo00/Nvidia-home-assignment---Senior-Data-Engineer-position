import os

import pandas as pd

from common.marker import this_method_is_only_for_test


class FileUtils:
    def read_file(self, file_path):
        raise NotImplementedError("Subclasses must implement read_file method")

    @staticmethod
    def get_last_file_modification(filenames_dir: str, filenames: [str]) -> int:
        last_modified = filenames[0]
        for file in filenames:
            if os.path.getmtime(f'{filenames_dir}/{last_modified}') <  os.path.getmtime(f'{filenames_dir}/{file}'):
                last_modified = file
        return int(os.path.getmtime(f'{filenames_dir}/{last_modified}')*1000000)

    @this_method_is_only_for_test
    def write_file(self, filename, data):
        """ This method is only used by unit tests """
        raise NotImplementedError("Subclasses must implement read_file method")



