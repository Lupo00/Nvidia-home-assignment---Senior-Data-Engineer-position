import logging
import os

import pandas as pd

from common.file_utils.CSVUtils import CSVUtils
from common.file_utils.JSONUtils import JSONUtils
from common.file_utils.LogUtils import LogUtils
from common.file_utils.XMLParser import XMLUtils
from common.logger.nvidia_logger import NvidiaLogger


class FileUtilsFactory:
    @staticmethod
    def create_utils(file_path):
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.csv':
            return CSVUtils()
        elif file_extension == '.json':
            return JSONUtils()
        elif file_extension == '.xml':
            return XMLUtils()
        elif file_extension == '.log':
            return LogUtils()
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

    @staticmethod
    def load_files_to_df(dir: str, filenames: [], to_add_file_information=False) -> pd.DataFrame:
        dataframes = []
        for filename in filenames:
            try:
                file_utils = FileUtilsFactory.create_utils(f'{dir}/{filename}')
                df = file_utils.read_file(f'{dir}/{filename}')
                if to_add_file_information:
                    df['source_file'] = filename.split(".")[0]
                    df['source_type'] = filename.split(".")[1]
                dataframes.append(df)
            except ValueError as e:
                print(f'File {filename} is not supported yet')
        return pd.concat(dataframes, ignore_index=True)

    @staticmethod
    def get_file_timestamp_which_changed_after_last_file(logger: NvidiaLogger, filenames: [str], filenames_path: str, sub_group_of_filesnames: [str]) -> [
        str]:
        last_file_timestamp = 0

        if sub_group_of_filesnames:
            last_file_timestamp = int(sorted(sub_group_of_filesnames)[-1].split(".json")[0].split("partition_")[1])
        logger.info(f"Last file modification timestamp at {sub_group_of_filesnames} is {last_file_timestamp}")

        files_sorted_and_filtered_by_last_file_timestamp = [file for file in filenames if
                                         int(os.path.getmtime(os.path.join(filenames_path, file))) > last_file_timestamp]
        logger.info(f"The files which exists in the system and grater than {last_file_timestamp} are {files_sorted_and_filtered_by_last_file_timestamp}")
        return files_sorted_and_filtered_by_last_file_timestamp
