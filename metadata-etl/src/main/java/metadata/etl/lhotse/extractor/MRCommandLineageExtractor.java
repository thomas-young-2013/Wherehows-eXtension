package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.XmlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by thomas young on 5/10/17.
 */
public class MRCommandLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MRCommandLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String params = xmlParser.getExtProperty("extProperties/entry/task.main.param");
            String outputDir = xmlParser.getExtProperty("extProperties/entry/mapred.output.dir");
            long flowExecId = Long.parseLong(xmlParser.getExtProperty("curRunDate"));

            String sourcePath = getSourcePath(params, outputDir);
            if (outputDir.endsWith("/")) outputDir = outputDir.substring(0, outputDir.length()-1);
            String destPath = outputDir + "/000000_0";

            // validation here
            if (sourcePath == null) return lineageRecords;
            // [++] need to add: check the source path and dest path: (/\\w+)+ regex

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String flowPath = "/lhotse/mr/" + flowExecId;
            String operation = "MR command";
            long num = 0L;

            logger.info("start to create the source record!");
            LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            // set lineage record details.
            lineageRecord.setDatasetInfo(defaultDatabaseId, sourcePath, "hdfs");
            lineageRecord.setOperationInfo("source", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            lineageRecord.setAbstractObjectName(sourcePath);
            lineageRecord.setFullObjectName(sourcePath);
            logger.info("the source record is: {}", lineageRecord.toDatabaseValue());
            lineageRecords.add(lineageRecord);

            logger.info("start to create the target record!");
            LineageRecord lineageRecord2 = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
            // set lineage record details.
            lineageRecord2.setDatasetInfo(defaultDatabaseId, destPath, "hdfs");
            lineageRecord2.setOperationInfo("target", operation, num, num,
                    num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
            lineageRecord2.setAbstractObjectName(destPath);
            lineageRecord2.setFullObjectName(destPath);
            logger.info("the target record is: {}", lineageRecord2.toDatabaseValue());
            lineageRecords.add(lineageRecord2);

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error happened in collecting lineage record.");
        }
        return lineageRecords;
    }

    private static String getSourcePath(String params, String destDir) {
        Pattern typePattern = Pattern.compile(".*\\.jar\\s+\\w+(\\.\\w+)+\\s+(.*)");
        Matcher typeMatcher = typePattern.matcher(params);
        String parameterSegment;
        if (typeMatcher.find()) {
            parameterSegment = typeMatcher.group(2);
        } else {
            return null;
        }
        String [] args = parameterSegment.split("\\s+");
        for (String path: args) {
            if (isPath(path) && !path.equals(destDir)) {
                return path;
            }
        }
        return null;
    }

    private static boolean isPath(String str) {
        Pattern typePattern = Pattern.compile("(/\\w+)+");
        Matcher typeMatcher = typePattern.matcher(str);
        if (typeMatcher.find()) return true;
        return false;
    }
}
