import pandas as pd
from common.file_utils.FileUtils import FileUtils
import xml.etree.ElementTree as ET

from common.marker import this_method_is_only_for_test


class XMLUtils(FileUtils):
    def read_file(self, file_path):
        return pd.read_xml(file_path)

    @this_method_is_only_for_test
    def write_file(self, filename, data):
        """ This method is only used by unit tests """
        root = ET.Element('products')
        for item in data:
            child = ET.Element('product')
            for key, val in item.items():
                subchild = ET.SubElement(child, key)
                subchild.text = str(val)
            root.append(child)

        tree = ET.ElementTree(root)
        tree.write(filename)