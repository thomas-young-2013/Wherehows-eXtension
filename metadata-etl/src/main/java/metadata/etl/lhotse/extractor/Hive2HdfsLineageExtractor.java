package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import wherehows.common.utils.ProcessUtils;
import wherehows.common.utils.XmlParser;
import metadata.etl.utils.hiveparser.HiveSqlAnalyzer;
import metadata.etl.utils.hiveparser.HiveSqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 3/31/17.
 */
public class Hive2HdfsLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(Hive2HdfsLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        if (lzTaskExecRecord.flowId == null) return lineageRecords;

        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String destPath = xmlParser.getExtProperty("extProperties/entry/destFilePath");
            String sql = xmlParser.getExtProperty("extProperties/entry/filterSQL");
            long flowExecId = lzTaskExecRecord.flowId;
            String databaseName = xmlParser.getExtProperty("extProperties/entry/databaseName");

            // get the hdfs file name.
            String [] cmds = {"hdfs", "dfs", "-ls", destPath};
            ArrayList<String> results = ProcessUtils.exec(cmds);
            // for debug
            logger.info("the process utils result: {}", results);
            if (results == null || results.size() == 0) {
                logger.error("process utils: no result get");
                return null;
            } else {
                if (!destPath.endsWith("/")) destPath += "/";
                String raw = results.get(results.size()-1);
                String []tmps = raw.split(" ");
                destPath = tmps[tmps.length - 1];
                // if (results.size() > 1) logger.info("process utils: result > 1");
            }

            logger.info("extract props from log file finished.");
            logger.info("the dest path is: {}", destPath);
            logger.info("the sql is: {}", sql);
            logger.info("the flow exce id is: {}", flowExecId);
            logger.info("the database name is: {}", databaseName);
            logger.info("the job name is: {}", lzTaskExecRecord.taskName);

            // parse the hive table from sql
            List<String> isrcTableNames = new ArrayList<String>();
            List<String> idesTableNames = new ArrayList<String>();
            if (sql != null) {
                String opType = HiveSqlAnalyzer.analyzeSql(sql, isrcTableNames, idesTableNames);
                if (opType.equals(HiveSqlType.QUERY)) {
                }
            }
            logger.info("hive sql parse finished.");

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            String operation = "hive2hdfs";
            long num = 0L;
            logger.info("start to create the source record: {}", isrcTableNames.toString());
            // source lineage record.
            for (String sourcePath : isrcTableNames) {
                LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                // set lineage record details.
                lineageRecord.setDatasetInfo(defaultDatabaseId, sourcePath, "hive");
                lineageRecord.setOperationInfo("source", operation, num, num,
                        num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                lineageRecord.setAbstractObjectName("/" + databaseName + "/" + sourcePath);
                lineageRecord.setFullObjectName("/" + databaseName + "/" + sourcePath);
                lineageRecord.setSrlNo(2);
                logger.info("the source record is: {}", lineageRecord.toString());
                lineageRecords.add(lineageRecord);
            }

            logger.info("start to create the target record!");
            // target lineage record.
            LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            // set lineage record details.
            lineageRecord.setDatasetInfo(defaultDatabaseId, destPath, "hdfs");
            lineageRecord.setOperationInfo("target", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            lineageRecord.setAbstractObjectName(destPath);
            lineageRecord.setFullObjectName(destPath);
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
