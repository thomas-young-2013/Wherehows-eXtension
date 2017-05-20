package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.ProcessUtils;
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
                                                int defaultDatabaseId, String logPath) {
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
            String destTmpPath = pathInfo.remove(pathInfo.size()-1);
            List<String> sourcePaths = new ArrayList<>();
            List<String> destPaths = new ArrayList<>();
            // get all input files.
            for (String path: pathInfo) {
                try {
                    if (isHdfsFile(path)) sourcePaths.add(path);
                    else sourcePaths.addAll(getSubFiles(path));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // get all output files.
            try {
                if (isHdfsFile(destTmpPath)) destPaths.add(destTmpPath);
                else destPaths.addAll(getSubFiles(destTmpPath));
            } catch (Exception e) {
                e.printStackTrace();
                return lineageRecords;
            }

            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            String operation = "spark submit";
            long num = 0L;

            logger.info("start to create the source record!");
            for (String sourcePath: sourcePaths) {
                LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                // set lineage record details.
                lineageRecord.setDatasetInfo(defaultDatabaseId, sourcePath, "hdfs");
                lineageRecord.setOperationInfo("source", operation, num, num,
                        num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                lineageRecord.setAbstractObjectName(sourcePath);
                lineageRecord.setFullObjectName(sourcePath);
                logger.info("the source record is: {}", lineageRecord.toDatabaseValue());
                lineageRecords.add(lineageRecord);
            }

            logger.info("start to create the target record!");
            for (String destPath: destPaths) {
                LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                // set lineage record details.
                lineageRecord.setDatasetInfo(defaultDatabaseId, destPath, "hdfs");
                lineageRecord.setOperationInfo("target", operation, num, num,
                        num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                lineageRecord.setAbstractObjectName(destPath);
                lineageRecord.setFullObjectName(destPath);
                logger.info("the target record is: {}", lineageRecord.toDatabaseValue());
                lineageRecords.add(lineageRecord);
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error happened in collecting lineage record.");
        }
        return lineageRecords;
    }

    private static void findPaths(String args, ArrayList<String> res) {
        Pattern typePattern = Pattern.compile("(--\\w+\\s+.*\\s+)*.*\\.jar\\s+(.*)");
        Matcher typeMatcher = typePattern.matcher(args);
        String parameterSegment = null;
        if (typeMatcher.find()) {
            parameterSegment = typeMatcher.group(2);
        }

        String [] paths = parameterSegment.split("\\s+");
        for (String path: paths) {
            if (path.matches("^(hdfs://hdfsCluster)?(/([a-z]|[A-Z]|[0-9]|-|\\.|\\*)+)+(/)?")) {
                if (path.startsWith("hdfs://hdfsCluster")) res.add(path.substring(18));
                else res.add(path);
            }
        }
    }

    private static boolean isHdfsFile(String path) throws Exception {
        // String [] cmds = {"hdfs", "dfs", "-test", "-f", path, "&&", "echo", "$?"};
        String [] cmds = {"hdfs", "dfs", "-ls", path};
        ArrayList<String> results = ProcessUtils.exec(cmds);
        // for debug
        logger.info("the process utils result: {}", results);
        if (results == null || results.size() == 0) {
            throw new Exception("getSubFiles: process utils no result get");
        } else {
            String [] arg = results.get(results.size()-1).split("\\s+");
            return arg.length == 8 && arg[7].equals(path);
        }
    }

    private static List<String> getSubFiles(String path) throws Exception {
        String [] cmds = {"hdfs", "dfs", "-ls", path};
        ArrayList<String> results = ProcessUtils.exec(cmds);
        List<String> dataPaths = new ArrayList<>();
        // for debug
        logger.info("the process utils result: {}", results);
        if (results == null || results.size() == 0) {
            throw new Exception("getSubFiles: process utils no result get");
        } else {
            for (String str: results) {
                String [] arg = str.split("\\s+");
                if (arg.length == 8 && !arg[4].equalsIgnoreCase("0") && !arg[7].startsWith("_")) {
                    dataPaths.add(arg[7]);
                }
            }
        }
        return dataPaths;
    }

}