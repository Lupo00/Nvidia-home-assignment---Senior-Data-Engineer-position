import argparse
import os
import sqlite3
import uuid

from common.file_utils.FileUtilsFactory import FileUtilsFactory
from common.logger.nvidia_logger import NvidiaLogger


def create_tables(cursor: sqlite3.Cursor) -> None:
    # Create a headers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS headers (
            header_id TEXT PRIMARY KEY, 
            part TEXT,
            serial_number TEXT,
            part_des TEXT,
            category TEXT,
            vendor TEXT,
            test_type TEXT,
            result_code INTEGER,
            result_description TEXT,
            test_date TEXT,
            station TEXT,
            source_type TEXT,
            source_file TEXT
        )
    ''')
    # Create a dynamic attribute table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dynamic (
            id INTEGER PRIMARY KEY, 
            header_id INTEGER,
            attribute_key TEXT,
            attribute_value TEXT,
            FOREIGN KEY (header_id) REFERENCES headers(header_id)
        )
    ''')


def loader_service(input_path:str, log_file:str ="std"):
    logger = NvidiaLogger(log_file)
    conn = sqlite3.connect('db/nvidia_products.db')
    cursor = conn.cursor()
    create_tables(cursor)
    logger.info("Tables header and dynamic are created at db/nvidia_products.db")
    input_filenames = os.listdir(input_path)
    if not input_filenames:
        logger.error(f"Directory {input_path} doest not contain any files")
        exit()
    logger.info(f"The files which will be insert are {input_filenames}")

    df = FileUtilsFactory.load_files_to_df(input_path, input_filenames)

    mandatory_columns = ['Part', 'serial_number', 'vendor', 'test_type', 'result_code', 'test_date']
    df['header_id'] = df[mandatory_columns].apply(
        lambda row: str(uuid.uuid5(uuid.NAMESPACE_DNS, '_'.join(map(str, row)))), axis=1)

    dynamic_attributes = [item for item in list(df.columns) if item.startswith("atter") or item == 'header_id']
    header_attributes = [item for item in list(df.columns) if not item.startswith("atter")]
    size_with_dups = df.shape[0]
    df = df.drop_duplicates('header_id')
    logger.info(f"We remove {size_with_dups-df.shape[0]} duplicate rows")
    try:
        dynamic_df_transformed = df[dynamic_attributes].melt(var_name='attribute_value', value_name='attribute_key',
                                                             id_vars=['header_id'])
        df[header_attributes].to_sql('headers', con=conn, index=False, if_exists='replace')
        dynamic_df_transformed.to_sql('dynamic', con=conn, index=False, if_exists='replace')
    except Exception as e:
        logger.error(e)


def unpack_arguments():
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('--input_path', '-i', type=str)
    parser.add_argument('--log_path', '-l', type=str, required=False)
    return parser.parse_args()


if __name__ == '__main__':
    args = unpack_arguments()
    loader_service(args.input_path, args.log_file)
