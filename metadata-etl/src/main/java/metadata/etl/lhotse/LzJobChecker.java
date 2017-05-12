/**
 * Copyright 2017 tencent. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package metadata.etl.lhotse;

import wherehows.common.utils.DateFormater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by thomas young on 3/30/17.
 */
public class LzJobChecker {
    final int DEFAULT_LOOK_BACK_TIME_MINUTES = 10;
    int appId;
    Connection conn = null;
    Connection conn1 = null;
    Connection conn2 = null;
    private static final Logger logger = LoggerFactory.getLogger(LzJobChecker.class);

    public LzJobChecker(Properties prop) throws SQLException {
        appId = Integer.valueOf(prop.getProperty(Constant.APP_ID_KEY));
        String host = prop.getProperty(Constant.LZ_DB_URL_KEY);
        String userName = prop.getProperty(Constant.LZ_DB_USERNAME_KEY);
        String passWord = prop.getProperty(Constant.LZ_DB_PASSWORD_KEY);
        // set up connections for lhotse database.
        conn = DriverManager.getConnection(host + "?" + "user=" + userName + "&password=" + passWord);

        // set up connections for tbds database.
        String tbdsUrl = prop.getProperty(Constant.TBDS_DB_URL_KEY);
        String tbdsUserName = prop.getProperty(Constant.TBDS_DB_USERNAME_KEY);
        String tbdsPassWord = prop.getProperty(Constant.TBDS_DB_PASSWORD_KEY);
        conn1 = DriverManager.getConnection(tbdsUrl + "?" + "user=" + tbdsUserName + "&password=" + tbdsPassWord);

        // set up connections for wh database
        String wherehowsUrl = prop.getProperty(Constant.WH_DB_URL_KEY);
        String wherehowsUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
        String wherehowsPassWord = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
        String connUrl = wherehowsUrl + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord;
        conn2 = DriverManager.getConnection(connUrl);
    }

    /**
     * Overload function getRecentFinishedJobFromFlow
     * @param timeFrameMinutes
     * @param endTimeStamp in milli second
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public List<LzTaskExecRecord> getRecentFinishedJobFromFlow(int timeFrameMinutes, long endTimeStamp)
            throws IOException, SQLException {
        logger.info("time frame is: " + timeFrameMinutes);
        long beginTimeStamp = endTimeStamp -  60 * timeFrameMinutes * 1000; // convert minutes to milli seconds
        return getRecentFinishedJobFromFlow(beginTimeStamp, endTimeStamp);
    }

    /**
     * Read the blob from "flow_data", do a topological sort on the nodes. Give them the sort id.
     * @param startTimeStamp the begin timestamp in milli seconds
     * @param endTimeStamp the end timestamp in milli seconds
     * @return
     */
    public List<LzTaskExecRecord> getRecentFinishedJobFromFlow(long startTimeStamp, long endTimeStamp)
            throws SQLException, IOException {

        logger.info("Get the jobs from time : {} to time : {}", startTimeStamp, endTimeStamp);
        String startTime = DateFormater.transform(startTimeStamp);
        String endTime = DateFormater.transform(endTimeStamp);
        logger.info("the time interval is: [" + startTime + " -> " + endTime + "]");

        List<LzTaskExecRecord> results = new ArrayList<>();
        Statement stmt = conn.createStatement();
        Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement();
        final String cmd = "select task_run.task_id, task_run.task_type, task_run.start_time, task_run.end_time, ref.task_name " +
                "from lb_task_run as task_run, lb_task as ref where task_run.start_time > \"%s\" " +
                "and task_run.end_time < \"%s\" and ref.task_id = task_run.task_id";
        final String cmd1 = "select wi.workflow_name, ti.project_name from task_info as ti, workflow_info wi " +
                "where ti.real_task_id = \"%s\" and ti.workflow_id = wi.workflow_id";
        final String cmd2 = "select flow_id from flow where flow_path = \"%s\"";

        logger.info("Get recent task sql : " + String.format(cmd, startTime, endTime));
        final ResultSet rs = stmt.executeQuery(String.format(cmd, startTime, endTime));

        // TO DO LIST: PROBLEMS MAY HAPPEN HERE.
        /*
        * topological sort.
        * */
        try {
            while (rs.next()) {
                String taskId = rs.getString("task_id");
                Integer typeId = rs.getInt("task_type");
                Integer taskStartTime = DateFormater.getInt(rs.getString("start_time"));
                Integer taskEndTime = DateFormater.getInt(rs.getString("end_time"));
                String taskName = rs.getString("task_name");

                LzTaskExecRecord lzTaskExecRecord = new LzTaskExecRecord(appId, taskId, typeId, taskName, taskStartTime, taskEndTime);
                results.add(lzTaskExecRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("read lhotse record from database: error found!");
        }

        // query workflow and project name from tbds database.
        for (LzTaskExecRecord lzTaskExecRecord: results) {
            String sql = String.format(cmd1, lzTaskExecRecord.taskId);
            final ResultSet resultSet = stmt1.executeQuery(sql);
            while (resultSet.next()) {
                String workflowName = resultSet.getString("workflow_name");
                String projectName = resultSet.getString("project_name");
                lzTaskExecRecord.projectName = projectName;
                lzTaskExecRecord.workflowName = workflowName;
            }
        }

        // query workflow_id from wherehows database.
        for (LzTaskExecRecord lzTaskExecRecord: results) {
            String flowPath = lzTaskExecRecord.projectName + ":" + lzTaskExecRecord.workflowName;
            final ResultSet resultSet = stmt2.executeQuery(String.format(cmd2, flowPath));
            while (resultSet.next()) {
                Integer flowId = resultSet.getInt("flow_id");
                lzTaskExecRecord.flowId = flowId;
            }
        }

        return results;
    }

    public void close()
            throws SQLException {
        conn.close();
    }
}
