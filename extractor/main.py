import argparse
import os

import numpy as np
import pandas as pd
import yaml

from common.file_utils.FileUtils import FileUtils
from common.file_utils.FileUtilsFactory import FileUtilsFactory
from common.logger.nvidia_logger import NvidiaLogger

corrupted_files_logs = NvidiaLogger(name="corrupted_files", log_file="logs/corrupted_files.log")


def is_column_not_presence(df: pd.DataFrame, col_schema: dict, target_col: str, supported_headers: set,
                           logger: NvidiaLogger):
    """Validate presence of required columns in DataFrame."""
    if not supported_headers:
        if 'mandatory' in col_schema:
            df['failed'] = True
        logger.error(f"{target_col} does not exist in the file")
        return True
    return False


def validate_column_types_and_content(df: pd.DataFrame, target_col: str, col_schema: dict):
    """Validate the data types of DataFrame columns based on schema"""
    if col_schema['type'] == 'int':
        df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
    elif col_schema['type'] == 'date':
        df[target_col] = pd.to_datetime(df[target_col], errors='coerce')


def mark_invalid_rows(df: pd.DataFrame, target_col: str, col_schema: dict, error_messages:dict):
    """Mark invalid rows in the DataFrame based on the schema."""
    condition = (df[target_col].isna() |
                 (df[target_col] == '') |
                 # (col_schema['type'] == 'int' and df[target_col] == -1) |
                 (col_schema['type'] == 'str' and (df[target_col].str.len() > col_schema['size'])))
    invalid_rows_df = df[condition]
    for idx, row in invalid_rows_df.iterrows():
        message = f"The value {row[target_col]} of {target_col}  is greater than expected size or is not exists.\n"
        if row[target_col] == '' or pd.isna(row[target_col]):
            message = f"The value of {target_col} ({row[target_col]}) is empty or in incorrect type.\n"
        if 'mandatory' in col_schema and col_schema['mandatory']:
            if idx not in error_messages:
                error_messages[idx] = [(row['source_file'],message)]
            else:
                error_messages[idx].append((row['source_file'],message))
            df.loc[idx, 'failed'] = True
        else:
            df.loc[idx, target_col] = np.nan


def filter_irrelevant_rows_and_columns(df, config):
    df = df[df['failed'] == False]
    columns_to_remove = [col for col in df.columns if col not in config.keys() and not col.startswith("atter")]
    corrupted_files_logs.info(f"We remove the columns {columns_to_remove} from the files")
    df.drop(columns=columns_to_remove, inplace=True)
    return df

def log_errors(error_messages):
    error_by_file = {}
    for idx, messages in error_messages.items():
        for message in messages:
            if message[0] not in error_by_file:
                error_by_file[message[0]] = {}
            error_by_file[message[0]][idx] = error_by_file[message[0]][idx] + [message[1]] if idx in error_by_file[
                message[0]] else [message[1]]
    for filename, content in error_by_file.items():
        if corrupted_files_logs.is_on_debug_mode():
            for row_error_desc in content.values():
                corrupted_files_logs.debug(f"In {filename} file - one row in have this issues : {row_error_desc}")
        corrupted_files_logs.info(f"In {filename} we have {len(content.keys())} rows which corrupted")
    if not corrupted_files_logs.is_on_debug_mode():
        corrupted_files_logs.info('To get information about every row (values and error description) you should change log level to DEBUG')


def reformat_and_validate_dataframe(logger: NvidiaLogger, schema_path: str, df: pd.DataFrame) -> pd.DataFrame:
    with open(schema_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    df['failed'] = False
    error_messages = {}
    for target_col, col_schema in config.items():
        supported_headers = set(col_schema.get("supported", []))
        supported_headers.add(target_col)
        supported_headers &= set(df.columns)

        if is_column_not_presence(df, col_schema, target_col, supported_headers, logger):
            continue

        df[target_col] = df[supported_headers].bfill(axis=1).iloc[:, 0]
        validate_column_types_and_content(df, target_col, col_schema,)
        mark_invalid_rows(df, target_col, col_schema, error_messages)

    log_errors(error_messages)
    df = filter_irrelevant_rows_and_columns(df, config)
    return df

def run_extractor(input_path: str, output_path: str, schema_path: str, batch_size: int = 10,
                  log_path: str = 'std') -> None:
    logger = NvidiaLogger(log_file=log_path)
    filenames_at_input_path = os.listdir(input_path)
    output_filenames = os.listdir(output_path)
    output_filenames = [filename for filename in output_filenames if filename.endswith(".xml") or
                                                                     filename.endswith(".csv") or
                                                                     filename.endswith(".log") or
                                                                     filename.endswith(".json")]
    # Get only files which not processed yet - Recovery Process
    filtered_filenames = FileUtilsFactory.get_file_timestamp_which_changed_after_last_file(logger, filenames_at_input_path,
                                                                                           input_path,
                                                                                           output_filenames)
    if not filtered_filenames:
        logger.info("Not found files to process")
        exit()

    # Divide the filenames into batches
    group_of_batch_files = [filtered_filenames[i:i + batch_size] for i in range(0, len(filtered_filenames), batch_size)]
    for batch_filenames in group_of_batch_files:
        try:
            logger.info(f"Reading {batch_filenames}")
            df = FileUtilsFactory.load_files_to_df(input_path, batch_filenames, True)
            df = reformat_and_validate_dataframe(logger, schema_path, df)
            last_time_file_modified = FileUtils.get_last_file_modification(input_path,batch_filenames)
            df.to_json(f"{output_path}/partition_{last_time_file_modified}.json", orient='records')
        except Exception as e:
            logger.error(e)


def unpack_arguments():
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('--batch_size', '-b', type=int, default=10)
    parser.add_argument('--input_path', '-s', type=str, default='source_files')
    parser.add_argument('--output_path', '-o', type=str, default='../formated_files')
    parser.add_argument('--schema_path', '-sc', type=str, default='config/schema.yaml')
    return parser.parse_args()


if __name__ == '__main__':
    args = unpack_arguments()
    run_extractor(args.input_path, args.output_path, args.schema_path)
