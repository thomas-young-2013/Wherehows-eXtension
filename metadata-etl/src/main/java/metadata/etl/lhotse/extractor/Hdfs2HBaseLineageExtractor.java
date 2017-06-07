package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.XmlParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lake on 17-6-6.
 */
public class Hdfs2HBaseLineageExtractor implements BaseLineageExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(Hdfs2HBaseLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message, int defaultDatabaseId, String logPath) {
        List<LineageRecord> lineageRecords = new ArrayList<>();
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        if (lzTaskExecRecord.flowId == null) return lineageRecords;


        try {
            LOG.info("HBase start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);

            String srcFile = xmlParser.getExtProperty("extProperties/entry/dataPath");
            String targetHBaseTable = xmlParser.getExtProperty("extProperties/entry/hbase.table");

            long flowExecId = lzTaskExecRecord.flowId;
            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String operation = "hdfs file data to hbase table";

            long num = 0L;

            /** src record */
            LineageRecord srcRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            srcRecord.setDatasetInfo(defaultDatabaseId, srcFile, "hdfs");
            srcRecord.setOperationInfo("source", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            srcRecord.setAbstractObjectName(srcFile);
            srcRecord.setFullObjectName(srcFile);
            LOG.info("the source record is: {}", srcRecord.toDatabaseValue());
            lineageRecords.add(srcRecord);


            /** target record */
            LineageRecord targetRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            targetRecord.setDatasetInfo(defaultDatabaseId, targetHBaseTable, "hbase");
            targetRecord.setOperationInfo("target", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            targetRecord.setAbstractObjectName("/" + targetHBaseTable);
            targetRecord.setFullObjectName("/" + targetHBaseTable);
            LOG.info("the source record is: {}", targetRecord.toDatabaseValue());
            lineageRecords.add(targetRecord);

            return lineageRecords;

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

        return lineageRecords;
    }
}
