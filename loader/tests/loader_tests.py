import os
import sqlite3
import unittest

from common.file_utils.FileUtilsFactory import FileUtilsFactory
from loader.main import loader_service


class LoaderTests(unittest.TestCase):
    def delete_all_tables(self):
        conn = sqlite3.connect('db/nvidia_products.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Drop each table individually
        for table_name in tables:
            sql = f"DROP TABLE IF EXISTS {table_name[0]}"
            cursor.execute(sql)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

    def test_the_basic_files_which_provided(self):
        self.delete_all_tables()
        loader_service('tests2_files')
        conn = sqlite3.connect('db/nvidia_products.db')
        cursor = conn.cursor()
        assert len(cursor.execute('SELECT source_file FROM headers').fetchall()) == 7
        assert len(cursor.execute('SELECT * FROM dynamic').fetchall()) == 28
        self.delete_all_tables()

    def test_process_avoid_duplicates_from_different_files(self):
        self.delete_all_tables()
        loader_service('tests1_files/iteration1')
        conn = sqlite3.connect('db/nvidia_products.db')
        cursor = conn.cursor()
        assert len(cursor.execute('SELECT source_file FROM headers').fetchall()) == 1
        assert cursor.execute('SELECT source_file FROM headers').fetchall()[0][0] == 'input3'

        # now we will check that the value is not duplicate and we update the source file which changed
        loader_service('tests1_files/iteration2')
        assert cursor.execute('SELECT COUNT(*) FROM headers').fetchall()[0][0] == 1
        assert cursor.execute('SELECT source_file FROM headers').fetchall()[0][0] == 'other_input'
        self.delete_all_tables()

    def test_insert_of_high_amount_of_data(self):
        self.delete_all_tables()
        df = FileUtilsFactory.load_files_to_df("tests3_files", os.listdir('tests3_files'))
        df_size = df.shape[0]
        loader_service('tests3_files')
        conn = sqlite3.connect('db/nvidia_products.db')
        cursor = conn.cursor()
        assert cursor.execute('SELECT COUNT(*) FROM headers').fetchall()[0][0] == 7599
        self.delete_all_tables()

        # assert len(cursor.execute('SELECT source_file FROM headers').fetchall()) == 1


if __name__ == '__main__':
    unittest.main()
