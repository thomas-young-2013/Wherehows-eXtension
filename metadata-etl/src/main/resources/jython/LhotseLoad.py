# author: thomas young 26/4/2017

from jython.SchedulerLoad import SchedulerLoad
import sys

class LhotseLoad(SchedulerLoad):
    def __init__(self, args):
        SchedulerLoad.__init__(self, args)

    def load_flows(self):
        # set flows that not in staging table to inactive
        cmd = """
          UPDATE flow f
          LEFT JOIN stg_flow s
          ON f.app_id = s.app_id AND f.flow_id = s.flow_id
          SET f.is_active = 'N'
          WHERE s.flow_id IS NULL AND f.app_id = {app_id}
          """.format(app_id=self.app_id)
        self.wh_cursor.execute(cmd)
        self.wh_con.commit()
        SchedulerLoad.load_flows(self)

if __name__ == "__main__":
    props = sys.argv[1]
    lz = LhotseLoad(props)
    lz.run()
