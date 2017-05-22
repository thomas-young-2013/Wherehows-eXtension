package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.ProcessUtils;
import wherehows.common.utils.XmlParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiahuilliu on 5/17/17.
 */
public class MRHdfsLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MRHdfsLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId, String logPath) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        try {
            logger.info("start to parse the logLocation: {}", logLocation);
            logger.info("start to parse the logPath: {}", logPath);
            File logfile = new File(logPath);
            BufferedReader br = new BufferedReader(new FileReader(logfile));
            LineNumberReader reader = new LineNumberReader(br);
            reader.setLineNumber(60);
            String str = "";
            String jobmark = "";  //info from logfile in location
            while ((jobmark = getJob_mark(reader.readLine())) != null) {
                break;
            }
            String targetfile = "/mr-history/done/";

            //get conf.xml by log info
            String realcom = ".*" + jobmark + "_conf.xml";
            String targetRaw = this.exeLsHdfs(targetfile, realcom);

            //get conf.xml from hdfs to local
            String[] cmdsget = {"hdfs", "dfs", "-get", targetRaw, "/tmp"};
            ArrayList<String> nullresults = ProcessUtils.exec(cmdsget);

            //analyse xml file from hdfs
            XmlParser xmlParser = new XmlParser("/tmp/" + targetRaw.substring(targetRaw.length() - 31));                                 //length =31
            logger.info("get info xml----------------------: {}", "/tmp/" + targetRaw.substring(targetRaw.length() - 31));

            String sourcePathdf = xmlParser.getExtProperty2("configuration/property/mapreduce.input.fileinputformat.inputdir");
            logger.info("the sourcePathdf is ----------------------------------------------------: {}", sourcePathdf);
            sourcePathdf=sourcePathdf.substring(18);
            String destPathdf = xmlParser.getExtProperty2("configuration/property/mapreduce.output.fileoutputformat.outputdir");
            destPathdf=destPathdf.substring(18);
            /*String comdest = ".*part.*";
            String destFilepath = this.exeLsHdfs(destDirpath, comdest);*/
            logger.info("the destPathdf is ----------------------------------------------------: {}", destPathdf);

            // get all input files.
            ArrayList<String> sourcePathfiles = new ArrayList<>();
            List<String> destPathfiles = new ArrayList<>();
            try {
                if (isHdfsFile(sourcePathdf)){
                    sourcePathfiles.add(sourcePathdf);
                }else {
                    sourcePathfiles.addAll(getSubFiles(sourcePathdf));
                }
                logger.info("the sourcePathfiles is ----------------------------------------------------: {}", sourcePathfiles.toArray().toString());
            }catch (Exception e){
                e.printStackTrace();
                logger.info("the sourcePathfiles is --------------------------------------------error--------: {}");
            }
            // get all output files.
            try {
                if (isHdfsFile(destPathdf)){
                    destPathfiles.add(destPathdf);
                }
                else {
                    destPathfiles.addAll(getSubFiles(destPathdf));
                }
                logger.info("the destPathfiles is ----------------------------------------------------: {}", destPathfiles.toArray().toString());
            } catch (Exception e) {
                e.printStackTrace();
                return lineageRecords;
            }

            //analyse file from locallog
            //XmlParser xmlParser2 = new XmlParser(logLocation);
            long flowExecId = lzTaskExecRecord.flowId;


            //common
            long taskId = Long.parseLong(lzTaskExecRecord.taskId);
            String taskName = lzTaskExecRecord.taskName;
            //String flowPath = "/lhotse/mr/" + flowExecId;
            String flowPath = String.format("%s:%s", lzTaskExecRecord.projectName, lzTaskExecRecord.workflowName);
            String operation = "MR command";
            long num = 0L;
            logger.info("start to create the source record!");
            for (String sourcePath: sourcePathfiles) {
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
            for (String destFilepath: destPathfiles) {
                LineageRecord lineageRecord = new LineageRecord(lzTaskExecRecord.appId, flowExecId, taskName, taskId);
                // set lineage record details.
                lineageRecord.setDatasetInfo(defaultDatabaseId, destFilepath, "hdfs");
                lineageRecord.setOperationInfo("target", operation, num, num,
                        num, num, lzTaskExecRecord.taskStartTime, lzTaskExecRecord.taskEndTime, flowPath);
                lineageRecord.setAbstractObjectName(destFilepath);
                lineageRecord.setFullObjectName(destFilepath);
                logger.info("the target record is: {}", lineageRecord.toDatabaseValue());
                lineageRecords.add(lineageRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error happened in collecting lineage record.");
        }
        return lineageRecords;
    }

    private String getJob_mark(String str) {
        Pattern pattern = Pattern.compile(".*+(job_\\d+_\\d+)");
        Matcher RLine = pattern.matcher(str);
        String job_mark;
        if (RLine.find()) {
            job_mark = RLine.group(1);
            return job_mark;
        }
        return null;
    }

    private boolean isJob_mark(String str, String com) {
        Pattern pattern = Pattern.compile(com);
        Matcher RLine = pattern.matcher(str);
        if (RLine.find()) {
            return true;
        }
        return false;
    }

    private String exeLsHdfs(String path, String com) {
        String[] cmds = {"hdfs", "dfs", "-lsr", path};
        ArrayList<String> results = ProcessUtils.exec(cmds);
        if (results == null || results.size() == 0) {
            logger.error("process utils: no result get");
            return null;
        } else {
            String raw = null;
            String targetRaw = null;
            String[] tmps;
            for (int i = 0; i < results.size(); i++) {
                raw = results.get(i);
                tmps = raw.split(" ");
                targetRaw = tmps[tmps.length - 1];
                if (this.isJob_mark(targetRaw, com)) {
                    break;
                }
            }
            return targetRaw;
        }
    }
    private static boolean isHdfsFile(String path) throws Exception {
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
