package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzTaskExecRecord;
import metadata.etl.utils.XmlParser;
import wherehows.common.schemas.LineageRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 3/31/17.
 */
public class Hive2HdfsLineageExtractor implements BaseLineageExtractor {

    public List<LineageRecord> getLineageRecord(String logLocation, LzTaskExecRecord lzTaskExecRecord,
                                                int defaultDatabaseId) {
        List<LineageRecord> lineageRecords = new ArrayList<>();
        XmlParser xmlParser = new XmlParser(logLocation);

        String destPath = xmlParser.getExtProperty("extProperties/entry/destFilePath");
        String sql = xmlParser.getExtProperty("extProperties/entry/filterSQL");
        String sourcePath = sql;

        long taskId = Long.parseLong(xmlParser.getExtProperty("id"));
        String taskName = "task_name"; // get from database table `lb_task`
        // task_name, task_id
        long flowExecId = Long.parseLong(xmlParser.getExtProperty("curRunDate"));

        // source lineage record.
        LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
        lineageRecord.setDatasetInfo(defaultDatabaseId, sourcePath, "hive");
        String flowPath = "";
        String operation = "";
        long num = 0L;
        lineageRecord.setOperationInfo("source", operation, num, num,
                        num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime,
                        flowPath);
        lineageRecords.add(lineageRecord);

        // target lineage record.
        LineageRecord lineageRecord1 = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
        lineageRecord1.setDatasetInfo(defaultDatabaseId, destPath, "hdfs");
        lineageRecord1.setOperationInfo("target", operation, num, num,
                num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
        lineageRecords.add(lineageRecord1);

        return lineageRecords;
    }
}
