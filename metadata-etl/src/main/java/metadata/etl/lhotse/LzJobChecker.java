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

import metadata.etl.utils.DateFormater;
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
    private static final Logger logger = LoggerFactory.getLogger(LzJobChecker.class);

    public LzJobChecker(Properties prop) throws SQLException {
        appId = Integer.valueOf(prop.getProperty(Constant.APP_ID_KEY));
        String host = prop.getProperty(Constant.LZ_DB_URL_KEY);
        String userName = prop.getProperty(Constant.LZ_DB_USERNAME_KEY);
        String passWord = prop.getProperty(Constant.LZ_DB_PASSWORD_KEY);
        // set up connections
        conn = DriverManager.getConnection(host + "?" + "user=" + userName + "&password=" + passWord);
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
        final String cmd = "select task_run.task_id, task_run.task_type, task_run.start_time, task_run.end_time, ref.task_name " +
                "from lb_task_run as task_run, lb_task as ref where task_run.start_time > \"%s\" " +
                "and task_run.end_time < \"%s\" and ref.task_id = task_run.task_id";
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
        return results;
    }

    public void close()
            throws SQLException {
        conn.close();
    }
}
