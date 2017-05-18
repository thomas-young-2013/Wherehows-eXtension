package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import wherehows.common.utils.XmlParser;
import metadata.etl.utils.hiveparser.HiveSqlAnalyzer;
import metadata.etl.utils.hiveparser.HiveSqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.FtpUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by thomas young on 5/8/17.
 */
public class HiveSqlLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(HiveSqlLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId, String logPath) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        if (lzTaskExecRecord.flowId == null) return lineageRecords;

        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String sqlFilePath = xmlParser.getExtProperty("extProperties/entry/sql.file.name");
            long flowExecId = lzTaskExecRecord.flowId;
            // split the file path
            int last_index = sqlFilePath.lastIndexOf("/");
            String sqlPath = sqlFilePath.substring(0, last_index + 1);
            String sqlFileName = sqlFilePath.substring(last_index + 1);

            logger.info("the sql filepath is: {}", sqlFilePath);
            logger.info("flow exec id is: {}", flowExecId);
            logger.info("the sql file path is: {}", sqlPath);
            logger.info("the sql file name is: {}", sqlFileName);

            String ftpHost = message.prop.getProperty(Constant.FTP_HOST_KEY);
            int port = Integer.parseInt(message.prop.getProperty(Constant.FTP_PORT));
            String userName = message.prop.getProperty(Constant.FTP_USERNAME_KEY);
            String password = message.prop.getProperty(Constant.FTP_PASSWORD_KEY);

            // read sql statements from sql file on ftp
            List<String> sqlList = FtpUtils.getFileContent(ftpHost, port, userName, password,
                    sqlPath, sqlFileName);
            if (sqlList == null) {
                logger.error("get sql script from ftp failed!");
                return lineageRecords;
            }
            if (sqlList.size() == 0) return lineageRecords;
            String sqlSentence = sqlList.get(0);
            for (int i=1; i<sqlList.size(); i++) sqlSentence += "\n" + sqlList.get(i);
            // get sql statements, rather than row.
            String [] sqls = sqlSentence.split(";");
            logger.info("the sql sentecnes are: {}", sqls);

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;
            // it need to consider...
            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            // important
            String operation = "hive sql script";
            // need to think about.
            long num = 0L;
            String databaseName = "default";

            for (String sql: sqls) {
                logger.info("start to parse sql: {}", sql);
                List<String> isrcTableNames = new ArrayList<String>();
                List<String> idesTableNames = new ArrayList<String>();
                String opType = HiveSqlAnalyzer.analyzeSql(sql, isrcTableNames, idesTableNames);
                logger.info("src tables : {}", isrcTableNames);
                logger.info("des tables : {}", idesTableNames);

                if (opType.equals(HiveSqlType.SWITCHDB)) {
                    databaseName = idesTableNames.get(0);
                    continue;
                }
                // escape the sql that does not affect the lineage.
                if (!opType.equals(HiveSqlType.INSERT) && !opType.equals(HiveSqlType.LOAD)
                        && !opType.equals(HiveSqlType.CREATETB)) continue;

                // if create a table with no source table, then no lineage need to extract.
                if (opType.equals(HiveSqlType.CREATETB) && isrcTableNames.size() == 0) continue;

                // first decide the type of source path: hdfs or hive.
                String storageType = "hive";
                // ignore the file in the fs except HDFS.
                if (opType.equals(HiveSqlType.LOAD)) {
                    Pattern typePattern = Pattern.compile("local\\s+inpath");
                    Matcher typeMatcher = typePattern.matcher(sql);
                    if (!typeMatcher.find()) storageType = "hdfs";
                    else continue;
                }

                for (String sourcePath : isrcTableNames) {
                    LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                    // set lineage record details.
                    lineageRecord.setDatasetInfo(defaultDatabaseId, sourcePath, storageType);
                    lineageRecord.setOperationInfo("source", operation, num, num,
                            num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                    if (storageType.equals("hive")) {
                        String dbName = getDatabase(sourcePath, sql);
                        String path = String.format("/%s/%s", dbName == null?databaseName:dbName, sourcePath);
                        lineageRecord.setAbstractObjectName(path);
                        lineageRecord.setFullObjectName(path);
                    } else {
                        lineageRecord.setAbstractObjectName(sourcePath);
                        lineageRecord.setFullObjectName(sourcePath);
                    }
                    logger.info("add source record is: {}", lineageRecord.toDatabaseValue());
                    lineageRecords.add(lineageRecord);
                }

                for (String destPath: idesTableNames) {
                    // target lineage record.
                    LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                    // set lineage record details.
                    lineageRecord.setDatasetInfo(defaultDatabaseId, destPath, "hive");
                    lineageRecord.setOperationInfo("target", operation, num, num,
                            num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                    String dbName = getDatabase(destPath, sql);
                    String path = String.format("/%s/%s", dbName == null?databaseName:dbName, destPath);
                    lineageRecord.setAbstractObjectName(path);
                    lineageRecord.setFullObjectName(path);
                    logger.info("add target record is: {}", lineageRecord.toDatabaseValue());
                    lineageRecords.add(lineageRecord);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineageRecords;
    }

    private static String getDatabase(String table, String sql) {
        String regex = String.format("(\\w+)\\.%s", table);
        Pattern typePattern = Pattern.compile(regex);
        Matcher typeMatcher = typePattern.matcher(sql);
        if (typeMatcher.find()) {
            return typeMatcher.group(1);
        }
        return null;
    }
}
