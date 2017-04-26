# author: thomas young 26/4/2017

from wherehows.common.writers import FileWriter
from wherehows.common import Constant
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import os, sys, json, gzip
import StringIO
import datetime, time
import DbUtil


class LhotseExtract:

    _period_unit_table = {'d': 'DAY',
                          'M': 'MONTH',
                          'h': 'HOUR',
                          'm': 'MINUTE',
                          'w': 'WEEK'}

    def __init__(self, args):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
        self.app_id = int(args[Constant.APP_ID_KEY])
        self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
        self.lz_con = zxJDBC.connect(args[Constant.LZ_DB_URL_KEY],
                                     args[Constant.LZ_DB_USERNAME_KEY],
                                     args[Constant.LZ_DB_PASSWORD_KEY],
                                     args[Constant.LZ_DB_DRIVER_KEY])
        self.lz_cursor = self.lz_con.cursor()
        self.lookback_period = args[Constant.LZ_EXEC_ETL_LOOKBACK_MINS_KEY]
        self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
        self.metadata_folder = self.app_folder + "/" + str(SchedulerType.LHOTSE) + "/" + str(self.app_id)

        if not os.path.exists(self.metadata_folder):
            try:
                os.makedirs(self.metadata_folder)
            except Exception as e:
                self.logger.error(e)

    def run(self):
        self.logger.info("Begin Lhotse Extract")
        try:
            # to do list
            pass
        finally:
            self.lz_cursor.close()
            self.lz_con.close()

if __name__ == "__main__":
    props = sys.argv[1]
    lz = LhotseExtract(props)
    lz.run()