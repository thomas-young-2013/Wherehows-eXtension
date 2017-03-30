package metadata.etl.lhotse;

import metadata.etl.lineage.AzJobChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hadoop on 3/30/17.
 */
public class LzJobChecker {
    final int DEFAULT_LOOK_BACK_TIME_MINUTES = 10;
    int appId;
    Connection conn = null;
    private static final Logger logger = LoggerFactory.getLogger(AzJobChecker.class);

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
        List<LzTaskExecRecord> results = new ArrayList<>();
        Statement stmt = conn.createStatement();
        final String cmd =
                "select * from lb_task_run where end_time > " + startTimeStamp
                        + " and end_time < " + endTimeStamp;
        logger.info("Get recent task sql : " + cmd);
        final ResultSet rs = stmt.executeQuery(cmd); // this sql take 3 second to execute

        while (rs.next()) {
            String taskId = rs.getString("task_id");
            Integer typeId = rs.getInt("type_id");
            LzTaskExecRecord lzTaskExecRecord = new LzTaskExecRecord(appId, taskId, typeId);
            results.add(lzTaskExecRecord);
        }
        return results;
    }

    public void close()
            throws SQLException {
        conn.close();
    }
}
