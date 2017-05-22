package metadata.etl.lhotse.crawler;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.utils.FileOperator;
import wherehows.common.utils.SshUtils;

import java.io.File;
import java.util.Arrays;

/**
 * Created by hadoop on 5/22/17.
 */
public class LhotseMRLogCrawler implements BaseCrawler {
    private static final Logger logger = LoggerFactory.getLogger(LhtoseConfCrawler.class);
    public static final String defaultLogLocation = "/usr/local/lhotse_runners/log/";

    @Override
    public String getRemoteLog(LzExecMessage message) throws Exception {
        LzTaskExecRecord lzRecord = message.lzTaskExecRecord;
        String logPath = null;

        if (message.prop.getProperty(Constant.LZ_LINEAGE_LOG_REMOTE, "false").equalsIgnoreCase("false")) {
            logPath = message.prop.getProperty(Constant.LZ_LINEAGE_LOG_DEFAULT_DIR, defaultLogLocation);
            logPath += String.format("%d/%s/%s", lzRecord.taskType,
                    lzRecord.taskId.substring(lzRecord.taskId.length() - 2), lzRecord.taskId);
            String mrfileName = FileOperator.getOneLogFile(logPath);
            logPath += "/" + mrfileName;
        }else {
            String remoteUser = message.prop.getProperty(Constant.LZ_REMOTE_USER_KEY);
            String remoteHost = message.prop.getProperty(Constant.LZ_REMOTE_MACHINE_KEY);
            String keyLocation = message.prop.getProperty(Constant.LZ_PRIVATE_KEY_LOCATION_KEY);
            String localLogPathFile = message.prop.getProperty(Constant.LZ_LINEAGE_LOG_DEFAULT_DIR);
            // move the log file from remote host to local host
            String remoteLogPath = message.prop.getProperty(Constant.LZ_REMOTE_LOG_DIR, defaultLogLocation);
            remoteLogPath += String.format("%d/%s/%s", lzRecord.taskType,
                    lzRecord.taskId.substring(lzRecord.taskId.length() - 2), lzRecord.taskId);
            // get the file list in the remote directory.
            String fileList2 = SshUtils.exec(remoteHost, remoteUser, keyLocation, "ls " + remoteLogPath);
            String []files2 = fileList2.split(" ");
            if (files2.length > 0) {
                Arrays.sort(files2);
            } else {
                logger.error("no log file found! task_id is: {}", message.lzTaskExecRecord.taskId);
                return null;
            }
            // prepare the remote log file.
            String remoteLogFileName2 = files2[files2.length - 1];
            String remoteLogFile2 = String.format("%s/%s", remoteLogPath, remoteLogFileName2);
            // prepare the local log file.
            localLogPathFile += String.format("%d/%s/%s/", lzRecord.taskType,
                    lzRecord.taskId.substring(lzRecord.taskId.length() - 2), lzRecord.taskId);
            new File(localLogPathFile).mkdirs();

            // fetch the remote log file to local directory.
            logger.info("local log directory is: {}", localLogPathFile);
            SshUtils.fileFetch(remoteHost, remoteUser, keyLocation, remoteLogFile2, localLogPathFile);
            logPath = localLogPathFile + remoteLogFileName2;
        }
        if (logPath == null) {
            logger.error("log file location error!");
            throw new Exception("log file location error!");
        } else {
            logger.info("log file to parse: {}", logPath);
        }
        return logPath;
    }

}
