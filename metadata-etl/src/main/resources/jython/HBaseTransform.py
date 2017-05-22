## creator: thomas young 22/5/2017
# -*-coding:utf-8 -*-
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import sys
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

class HBaseTransform:
    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    def transform(self, raw_metadata, metadata_output, field_metadata_output):
        input_json_file = open(raw_metadata, 'r')
        schema_file_writer = FileWriter(metadata_output)
        field_file_writer = FileWriter(field_metadata_output)

    if __name__ == "__main__":
        args = sys.argv[1]
        # parse the arguments and do transformation.