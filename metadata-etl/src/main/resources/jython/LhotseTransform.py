## author: thomas young 26/4/2017
# -*-coding:utf-8 -*-
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
from jython.SchedulerTransform import SchedulerTransform
from wherehows.common.enums import SchedulerType
import sys
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

class LhotseTransform(SchedulerTransform):
    SchedulerTransform._tables["flows"]["columns"] = "app_id, flow_name, flow_group, flow_path, flow_level, source_created_time, source_modified_time, source_version, is_active, wh_etl_exec_id"
    SchedulerTransform._tables["jobs"]["columns"] = "app_id, flow_path, source_version, job_name, job_path, job_type, ref_flow_path, is_current, wh_etl_exec_id"
    SchedulerTransform._tables["owners"]["columns"] = "app_id, flow_path, owner_id, permissions, owner_type, wh_etl_exec_id"
    SchedulerTransform._tables["flow_execs"]["columns"] = "app_id, flow_name, flow_path, source_version, flow_exec_id, flow_exec_status, attempt_id, executed_by, start_time, end_time, wh_etl_exec_id"
    SchedulerTransform._tables["job_execs"]["columns"] = "app_id, flow_path, source_version, flow_exec_id, job_name, job_path, job_exec_id, job_exec_status, attempt_id, start_time, end_time, wh_etl_exec_id"

    update_flow_id_templates = """
                              UPDATE {table} SET flow_exec_id = flow_id WHERE app_id = {app_id}
                              """
    update_job_id_templates =  """
                              UPDATE {table} SET flow_exec_id = flow_id WHERE app_id = {app_id}
                              """
    def __init__(self, args):
        SchedulerTransform.__init__(self, args, SchedulerType.LHOTSE)
        self.wh_con_1 = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                     args[Constant.WH_DB_USERNAME_KEY],
                                     args[Constant.WH_DB_PASSWORD_KEY],
                                     args[Constant.WH_DB_DRIVER_KEY])
        self.wh_cursor_1 = self.wh_con_1.cursor()

    def update_exec_id(self):
        try:
            query = self.update_flow_id_templates.format(table="stg_flow_execution", app_id=self.app_id)
            self.logger.debug(query)
            self.wh_cursor_1.execute(query)
            self.wh_con_1.commit()

            query = self.update_job_id_templates.format(table="stg_job_execution", app_id=self.app_id)
            self.logger.debug(query)
            self.wh_cursor_1.execute(query)
            self.wh_con_1.commit()
        finally:
            self.wh_cursor_1.close()
            self.wh_con_1.close()

if __name__ == "__main__":
    props = sys.argv[1]
    lz = LhotseTransform(props)
    lz.run()
    lz.update_exec_id()
