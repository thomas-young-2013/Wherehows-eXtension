## creator: thomas young 22/5/2017
# -*-coding:utf-8 -*-
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import sys
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

class HBaseLoad:
    def __init__(self, exec_id):
        self.logger = LoggerFactory.getLogger(self.__class__.__name__ + ':' + str(exec_id))

    def load_metadata(self):
        """
        Load dataset metadata into final table
        :return: nothing
        """
        cursor = self.conn_mysql.cursor()
        self.logger.info("finish loading hbase metadata db_id={db_id} to dict_dataset".format(db_id=self.db_id))

    def load_field(self):
        cursor = self.conn_mysql.cursor()
        self.logger.info("finish loading hbase metadata db_id={db_id} to dict_field_detail".format(db_id=self.db_id))

    def load_sample(self):
        cursor = self.conn_mysql.cursor()
        self.logger.info("finish loading hbase sample data db_id={db_id} to dict_dataset_sample".format(db_id=self.db_id))

if __name__ == "__main__":
    args = sys.argv[1]

    l = HBaseLoad(args[Constant.WH_EXEC_ID_KEY])

    # set up connection
    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]
    l.input_file = args[Constant.HDFS_SCHEMA_RESULT_KEY]
    l.input_field_file = args[Constant.HDFS_FIELD_RESULT_KEY]
    l.input_sample_file = args[Constant.HDFS_SAMPLE_LOCAL_PATH_KEY]

    l.db_id = args[Constant.DB_ID_KEY]
    l.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    l.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
