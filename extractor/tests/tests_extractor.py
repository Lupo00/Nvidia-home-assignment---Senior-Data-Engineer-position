import json
import os
import shutil
import unittest

import pandas as pd

from common.file_utils.FileUtilsFactory import FileUtilsFactory
from common.logger.nvidia_logger import NvidiaLogger

from extractor.main import run_extractor

logger = NvidiaLogger(log_file='logs/system.log')
corrupted_files_logs = NvidiaLogger(name="corrupted_files", log_file="logs/corrupted_files.log")

from extractor.tests import files_generator


class ExtractorTests(unittest.TestCase):
    @staticmethod
    def create_dir_path_and_reset_if_exists( path):
        path_parts = path.split("/")
        cur_path = ""
        for part in path_parts:
            cur_path = cur_path + "/" + part if cur_path is not "" else part
            if not os.path.exists(cur_path):
                os.makedirs(cur_path)
        shutil.rmtree(path)
        os.mkdir(path)

    @corrupted_files_logs.mark_start_and_end_run("Test basic example")
    @logger.mark_start_and_end_run("Test basic example")
    def test_basic_example(self):
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test1_files/output')
        run_extractor(input_path='../source_files', output_path="test_files/test1_files/output",
                      schema_path='../config/schema.yaml')
        files = os.listdir('test_files/test1_files/output')
        files = [file for file in files if
                 file.endswith('.csv') or file.endswith('.log') or file.endswith('.xml') or file.endswith('.json')]
        dfs = []
        for file in files:
            dfs.append(pd.read_json(f'test_files/test1_files/output/{file}'))
        df = pd.concat(dfs, ignore_index=True)
        assert df.shape[0] == 9  # 5 Invalid fields , but only 4 mandatory ( in total we have 13 rows - so 9)

    @corrupted_files_logs.mark_start_and_end_run("Test recovery from failure")
    @logger.mark_start_and_end_run("Test recovery from failure")
    def test_recovery_from_failure(self):
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/tests2_files/output')
        run_extractor(input_path='../source_files', output_path="test_files/tests2_files/output",
                      schema_path='../config/schema.yaml')
        try:
            run_extractor(input_path='../source_files', output_path="test_files/tests2_files/output",
                          schema_path='../config/schema.yaml')
        except SystemExit as e:
            print("The list of files to process is none as expexted")

        files = os.listdir('test_files/tests2_files/output')
        files = [file for file in files if
                 file.endswith('.csv') or file.endswith('.log') or file.endswith('.xml') or file.endswith('.json')]
        dfs = []
        for file in files:
            dfs.append(pd.read_json(f'test_files/tests2_files/output/{file}'))
        df = pd.concat(dfs, ignore_index=True)
        assert df.shape[0] == 9

    @corrupted_files_logs.mark_start_and_end_run("Test less than one value per column")
    @logger.mark_start_and_end_run("Test less than one value per column")
    def test_less_than_one_value_per_column(self):
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test4_files/source')
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test4_files/output')
        data_without_part_col = [{
            "product_name": "jaQcMelwZKjnIEuWLjhMKqTjJUlDurjexgwLDWwHIFbIGI",
            "sn": "qnH",
            "Category": "TcLxCRqnOZtVSrsvVuRuxzscpfesTLPLcdLhMfvDjQGNEUCtIKZYqigy",
            "manufacturer": "i",
            "TestType": "VOBeaXHYFonhK",
            "TestResult": 77,
            "TestResultDesc": "tHjdw",
            "test_date": "2024-04-29 09:01:49",
            "Station": "UtlCJ"
        }]
        with open('test_files/test4_files/source/test.json', 'w') as json_file:
            json.dump(data_without_part_col, json_file)
        run_extractor(input_path='test_files/test4_files/source', output_path="test_files/test4_files/output",
                      schema_path='../config/schema.yaml')
        files = os.listdir('test_files/test4_files/output')

        files = [file for file in files if
                 file.endswith('.csv') or file.endswith('.log') or file.endswith('.xml') or file.endswith('.json')]
        with open(f'test_files/test4_files/output/{files[0]}', "r") as json_file:
            data = json.load(json_file)
            assert len(data) == 0

    @corrupted_files_logs.mark_start_and_end_run("Test high amount of data")
    @logger.mark_start_and_end_run("Test high amount of data")
    def test_high_amount_of_data(self):
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test3_files/output')
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test3_files/source')
        ExtractorTests.create_dir_path_and_reset_if_exists('test_files/test3_files/failures_counter')
        files_generator.run(min_rows=100, max_rows=500, amount_of_files=30)
        run_extractor(input_path='test_files/test3_files/source', output_path="test_files/test3_files/output",
                      schema_path='../config/schema.yaml')
        files = os.listdir('test_files/test3_files/output')
        files = [file for file in files if
                 file.endswith('.csv') or file.endswith('.log') or file.endswith('.xml') or file.endswith('.json')]

        dfs = []
        for file in files:
            dfs.append(pd.read_json(f'test_files/test3_files/output/{file}'))
        df_formated_files = pd.concat(dfs, ignore_index=True)
        df_source_files = FileUtilsFactory.load_files_to_df('test_files/test3_files/source',
                                                            os.listdir('test_files/test3_files/source'))

        df_source_files_size = df_source_files.shape[0]
        df_formated_files_size = df_formated_files.shape[0]
        failures_counter_files = os.listdir('test_files/test3_files/failures_counter')
        failures_counter_files = [file for file in failures_counter_files if file.endswith('.txt')]

        size_of_failures_rows = 0
        for file in failures_counter_files:
            with open(f'test_files/test3_files/failures_counter/{file}') as f:
                size_of_failures_rows += int(f.readline().strip())

        print(f'df_formated_files_size= {df_formated_files_size}')
        print(f'df_source_files_size= {df_source_files_size}')
        print(f'size_of_failures_rows= {size_of_failures_rows}')
        ratio = 1 - df_formated_files_size / (df_source_files_size - size_of_failures_rows)
        print(ratio)
        assert 0.001 > ratio > -0.999


if __name__ == '__main__':
    unittest.main()
