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

import static wherehows.common.utils.RegexUtils.isPath;

/**
 * Created by thomas young on 5/10/17.
 */
public class SparkSubmitLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(SparkSubmitLineageExtractor.class);

    /*
    * [++] this class need to be rewritten in the latter.
    * */
    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        List<LineageRecord> lineageRecords = new ArrayList<>();
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        if (lzTaskExecRecord.flowId == null) return lineageRecords;

        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String shellArgs = xmlParser.getExtProperty("extProperties/entry/shell.args");
            long flowExecId = lzTaskExecRecord.flowId;

            ArrayList<String> pathInfo = new ArrayList<>();
            findPaths(shellArgs, pathInfo);
            if (pathInfo.size() < 2) return lineageRecords;
            String sourcePath = filter(pathInfo.get(0));
            String destPath = filter(pathInfo.get(1));

            if (!destPath.endsWith("/")) destPath += "/";
            destPath += "part-00000";

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            String operation = "spark submit";
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

    private void findPaths(String args, ArrayList<String> res) {
        Pattern typePattern = Pattern.compile("(--\\w+\\s+.*\\s+)*.*\\.jar\\s+(.*)");
        Matcher typeMatcher = typePattern.matcher(args);
        String parameterSegment = null;
        if (typeMatcher.find()) {
            parameterSegment = typeMatcher.group(2);
        }

        String [] paths = parameterSegment.split("\\s+");
        for (String path: paths) {
            if (isPath(path)) res.add(path);
        }
    }

    private String filter(String path) {
        String res = path.trim();
        if (res.startsWith("hdfs://hdfsCluster")) {
            return res.substring(18);
        }
        return res;
    }
}
