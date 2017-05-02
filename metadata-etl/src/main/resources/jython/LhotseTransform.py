# -*-coding:utf-8 -*-
## author: thomas young 26/4/2017

from jython.SchedulerTransform import SchedulerTransform
from wherehows.common.enums import SchedulerType
import sys

class LhotseTransform(SchedulerTransform):
    SchedulerTransform._tables["flows"]["columns"] = "app_id, flow_name, flow_group, flow_path, flow_level, source_modified_time, source_version, is_active, wh_etl_exec_id"
    SchedulerTransform._tables["jobs"]["columns"] = "app_id, flow_path, source_version, job_name, job_path, job_type, ref_flow_path, is_current, wh_etl_exec_id"
    SchedulerTransform._tables["owners"]["columns"] = "app_id, flow_path, owner_id, permissions, owner_type, wh_etl_exec_id"
    SchedulerTransform._tables["flow_execs"]["columns"] = "app_id, flow_name, flow_path, source_version, flow_exec_id, flow_exec_status, attempt_id, executed_by, start_time, end_time, wh_etl_exec_id"
    SchedulerTransform._tables["job_execs"]["columns"] = "app_id, flow_path, source_version, flow_exec_id, job_name, job_path, job_exec_id, job_exec_status, attempt_id, start_time, end_time, wh_etl_exec_id"

    def __init__(self, args):
        SchedulerTransform.__init__(self, args, SchedulerType.LHOTSE)

if __name__ == "__main__":
    ## set the encodings
    reload(sys)
    sys.setdefaultencoding('utf-8')

    props = sys.argv[1]
    lz = LhotseTransform(props)
    lz.run()
