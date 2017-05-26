package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.ProcessUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by thomas young on 5/26/17.
 */
public class SparkLineageExtractor implements BaseLineageExtractor {
    private static final Logger logger = LoggerFactory.getLogger(SparkLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId, String logPath) {
        List<LineageRecord> lineageRecords = new ArrayList<>();
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<String> sourcePaths = new ArrayList<>();
        List<String> destPaths = new ArrayList<>();

        try {
            // iterate the log file and match the regex to get the input, output data set.
            List<String> sourceFiles = new ArrayList<>();
            List<String> destFiles = new ArrayList<>();
            extractInputandOutput(logPath, sourceFiles, destFiles);

            // add all source files.
            for (String path: sourceFiles) {
                if (isHdfsFile(path)) sourcePaths.add(path);
            }

            // add all destination files.
            for (String path: destFiles) {
                if (isHdfsFile(path)) destPaths.add(path);
                else destPaths.addAll(getSubFiles(path));
            }

            long flowExecId = lzTaskExecRecord.flowId;
            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;

            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            String operation = "spark task";
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

    private static void extractInputandOutput(String logFile, List<String> sourcePaths, List<String> destPaths) {
        Pattern sourcePattern = Pattern.compile("Input split: hdfs://(\\w+|(\\d+.){3}\\d+:\\d+)" +
                "((/([a-z]|[A-Z]|[0-9]|-|\\.|\\*)+)+(/)?)");
        Pattern targetPattern = Pattern.compile("Saved output of task '.*' to hdfs://(\\w+|(\\d+.){3}\\d+:\\d+)" +
                "((/([a-z]|[A-Z]|[0-9]|-|\\.|\\*)+)+)/_temporary/");
        try {
            FileReader reader = new FileReader(logFile);
            BufferedReader br = new BufferedReader(reader);
            String str;
            while ((str = br.readLine()) != null) {
                // match source  path.
                Matcher sourceMatcher = sourcePattern.matcher(str);
                if (sourceMatcher.find()) {
                    String tmp = sourceMatcher.group(3);
                    if (!sourcePaths.contains(tmp)) sourcePaths.add(tmp);
                }

                // match target path.
                Matcher targetMatcher = targetPattern.matcher(str);
                if (targetMatcher.find()) {
                    String tmp = targetMatcher.group(3);
                    if (!destPaths.contains(tmp)) destPaths.add(tmp);
                }
            }

            br.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
