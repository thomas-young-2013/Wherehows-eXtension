## author: thomas young 26/4/2017
# -*-coding:utf-8 -*-
from wherehows.common.schemas import LhotseFlowRecord
from wherehows.common.schemas import LhotseJobRecord
from wherehows.common.schemas import LhotseFlowDagRecord
from wherehows.common.schemas import LhotseFlowOwnerRecord

from wherehows.common.writers import FileWriter
from wherehows.common import Constant
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import os, sys, json, gzip
import datetime, time
import DbUtil

class LhotseExtract:

    def __init__(self, args):

        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
        self.app_id = int(args[Constant.APP_ID_KEY])
        self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
        self.lz_con = zxJDBC.connect(args[Constant.LZ_DB_URL_KEY],
                                     args[Constant.LZ_DB_USERNAME_KEY],
                                     args[Constant.LZ_DB_PASSWORD_KEY],
                                     args[Constant.LZ_DB_DRIVER_KEY],
                                     charset='utf8')
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
            self.collect_flow_jobs(self.metadata_folder + "/flow.csv", self.metadata_folder + "/job.csv", self.metadata_folder + "/dag.csv")
            self.collect_flow_owners(self.metadata_folder + "/owner.csv")
            self.collect_flow_schedules(self.metadata_folder + "/schedule.csv")
            self.collect_flow_execs(self.metadata_folder + "/flow_exec.csv", self.metadata_folder + "/job_exec.csv", self.lookback_period)
        finally:
            self.lz_cursor.close()
            self.lz_con.close()

    def collect_flow_jobs(self, flow_file, job_file, dag_file):
        self.logger.info("collect flow&jobs")
        query = "SELECT * FROM workflow_info WHERE status is NULL"
        self.lz_cursor.execute(query)
        ## rows = DbUtil.dict_cursor(self.lz_cursor)
        rows = DbUtil.copy_dict_cursor(self.lz_cursor)
        flow_writer = FileWriter(flow_file)
        job_writer = FileWriter(job_file)
        dag_writer = FileWriter(dag_file)
        row_count = 0

        for row in rows:
            self.logger.info("collect flow %d!" % row_count)
            flow_path = row['project_name'] + ":" + row['workflow_name']
            print (flow_path)
            flow_record = LhotseFlowRecord(self.app_id,
                                            row['workflow_name'],
                                            row['project_name'],
                                            flow_path,
                                            0,
                                            int(time.mktime(row['modify_time'].timetuple())),
                                            0,
                                            'Y',
                                            self.wh_exec_id)
            ## for debug
            self.logger.info("the flow record is: %s" % flow_record.toCsvString())
            flow_writer.append(flow_record)

            # get relative task of this workflow.
            task_query = "SELECT * FROM task_info WHERE workflow_id = \"{0}\"".format(row['workflow_id'])
            new_lz_cursor = self.lz_cursor
            new_lz_cursor.execute(task_query)
            task_rows = DbUtil.dict_cursor(new_lz_cursor)

            for task in task_rows:
                job_record = LhotseJobRecord(self.app_id,
                                              flow_path,
                                              0,
                                              task['task_name'],
                                              flow_path + '/' + task['task_name'],
                                              task['task_type_name'],
                                              'Y',
                                              self.wh_exec_id)
                job_writer.append(job_record)

            # task bridge
            # bridge's status need to be considered in the next stage
            task_bridge_query = "SELECT * FROM task_bridge WHERE workflow_id = \"{0}\"".format(row['workflow_id'])
            self.lz_cursor.execute(task_bridge_query)
            task_bridge_rows = DbUtil.dict_cursor(self.lz_cursor)

            for bridge in task_bridge_rows:
                origin_task_query = "SELECT task_name FROM task_info WHERE task_id = \"{0}\"".format(bridge['origin_id'])
                self.lz_cursor.execute(origin_task_query)
                origin_tasks = self.lz_cursor.fetchone()

                target_task_query = "SELECT task_name FROM task_info WHERE task_id = \"{0}\"".format(bridge['target_id'])
                self.lz_cursor.execute(target_task_query)
                target_tasks = self.lz_cursor.fetchone()

                dag_edge = LhotseFlowDagRecord(self.app_id,
                                                flow_path,
                                                0,
                                                flow_path + '/' + origin_tasks[0],
                                                flow_path + '/' + target_tasks[0],
                                                self.wh_exec_id)
                dag_writer.append(dag_edge)

            row_count += 1

            if row_count % 1000 == 0:
                flow_writer.flush()
                job_writer.flush()
                dag_writer.flush()

        flow_writer.close()
        job_writer.close()
        dag_writer.close()

    def collect_flow_execs(self, flow_exec_file, job_exec_file, look_back_period):
        self.logger.info( "collect flow&job executions")
        flow_exec_writer = FileWriter(flow_exec_file)
        job_exec_writer = FileWriter(job_exec_file)
        flow_exec_writer.close()
        job_exec_writer.close()

    def collect_flow_schedules(self, schedule_file):
        # load flow scheduling info from table triggers
        self.logger.info("collect flow schedule")
        schedule_writer = FileWriter(schedule_file)
        schedule_writer.close()

    def collect_flow_owners(self, owner_file):
        # load user info from table project_permissions
        self.logger.info("collect owner&permissions")
        user_writer = FileWriter(owner_file)

        query = "SELECT project_name, workflow_name, owner FROM workflow_info WHERE status is NULL"
        self.lz_cursor.execute(query)
        rows = DbUtil.dict_cursor(self.lz_cursor)

        for row in rows:
            record = LhotseFlowOwnerRecord(self.app_id,
                                            row['project_name'] + ':' + row["workflow_name"],
                                            row["owner"],
                                            'ADMIN',
                                            'LDAP',
                                            self.wh_exec_id)
            user_writer.append(record)
        user_writer.close()

if __name__ == "__main__":
    ## set the encodings
    reload(sys)
    sys.setdefaultencoding('utf-8')

    props = sys.argv[1]
    lz = LhotseExtract(props)
    lz.run()