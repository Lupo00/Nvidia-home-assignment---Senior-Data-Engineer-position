import pandas as pd
from common.file_utils.FileUtils import FileUtils


class LogUtils(FileUtils):
    def read_file(self, file_path):
        rows = []
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                row = {}
                line = ":".join(line.split(":")[3:])
                for pair in line.split("|"):
                    _pair = pair.lstrip().rstrip().split(":")
                    if len(_pair) > 1:
                        row[_pair[0]] = ":".join(_pair[1:])
                if len(row) > 0:
                    rows.append(row)
        return pd.DataFrame(rows)