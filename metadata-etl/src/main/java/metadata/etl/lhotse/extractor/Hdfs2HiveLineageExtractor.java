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
 * Created by jiahuilliu on 5/8/17.
 */
public class Hdfs2HiveLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(Hdfs2HiveLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            long flowExecId = Long.parseLong(xmlParser.getExtProperty("curRunDate"));
            String sourcePath = xmlParser.getExtProperty("extProperties/entry/sourceFilePath");
            String databaseName = xmlParser.getExtProperty("extProperties/entry/databaseName");
            String tableName=xmlParser.getExtProperty("extProperties/entry/tableName");
            String sourceFileName=xmlParser.getExtProperty("extProperties/entry/sourceFileNames");

            logger.info("extract props from log file finished.");
            logger.info("the source path is: {}", sourcePath);
            logger.info("the database name is: {}", databaseName);

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;
            String flowPath = "/lhotse/hdfs2hive/" + flowExecId;
            String operation = null;

            // source lineage record.
            logger.info("start to create the source record!");
            long num = 0L;
            LineageRecord slineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            // set lineage record details.
            slineageRecord.setDatasetInfo(defaultDatabaseId,sourcePath+sourceFileName, "hdfs");
            slineageRecord.setOperationInfo("source", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            slineageRecord.setAbstractObjectName(sourcePath+sourceFileName);
            slineageRecord.setFullObjectName(sourcePath+sourceFileName);
            slineageRecord.setSrlNo(2);
            logger.info("the source record is: {}", slineageRecord.toString());

            lineageRecords.add(slineageRecord);



            // target lineage record.
            logger.info("start to create the target record!");
            // target lineage record.
            LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            // set lineage record details.
            lineageRecord.setDatasetInfo(defaultDatabaseId, tableName, "hive");
            lineageRecord.setOperationInfo("target", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            lineageRecord.setAbstractObjectName("/" + databaseName + "/" + tableName);
            lineageRecord.setFullObjectName("/" + databaseName + "/" + tableName);
            lineageRecord.setSrlNo(3);
            logger.info("the target record is: {}", lineageRecord.toString());
            lineageRecords.add(lineageRecord);


        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error happened in collecting lineage record.");
        }
        return lineageRecords;
    }
}
