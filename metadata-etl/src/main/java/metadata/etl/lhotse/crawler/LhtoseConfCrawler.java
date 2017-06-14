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
 * Created by thomas young on 5/22/17.
 */
public class LhtoseConfCrawler implements BaseCrawler {
    private static final Logger logger = LoggerFactory.getLogger(LhtoseConfCrawler.class);
    public static final String defaultRemoteConfLocation = "/usr/local/lhotse_runners/log/";
    public static final String defaultLocalConfLocation = "/var/tmp/wherehows/crawl_data/";

    @Override
    public String getRemoteLog(LzExecMessage message) throws Exception {
        LzTaskExecRecord lzRecord = message.lzTaskExecRecord;
        String localLogLocation = null;

        if (message.prop.getProperty(Constant.LZ_LINEAGE_LOG_REMOTE, "false").equalsIgnoreCase("false")) {
            // the full path
            localLogLocation = defaultRemoteConfLocation;
            localLogLocation += String.format("tasklog/%d/%s", lzRecord.taskType, lzRecord.taskId);
            // find the latest file name.
            String fileName = FileOperator.getOneLogFile(localLogLocation);
            localLogLocation += "/" + fileName;
        } else {
            String remoteUser = message.prop.getProperty(Constant.LZ_REMOTE_USER_KEY);
            // String remoteHost = message.prop.getProperty(Constant.LZ_REMOTE_MACHINE_KEY);
            String remoteHost = lzRecord.brokerId;
            logger.info("conf lhotse runner ip is: {}", remoteHost);
            String keyLocation = message.prop.getProperty(Constant.LZ_PRIVATE_KEY_LOCATION_KEY);
            // in remote mode, this field stands for the local dir to store the log files.
            // String localLogFile = message.prop.getProperty(Constant.LZ_LINEAGE_LOG_DEFAULT_DIR);
            String localLogFile = defaultLocalConfLocation;

            // move the log file from remote host to local host
            String remoteLogLocation = message.prop.getProperty(Constant.LZ_REMOTE_LOG_DIR, defaultRemoteConfLocation);
            remoteLogLocation += String.format("tasklog/%d/%s", lzRecord.taskType, lzRecord.taskId);
            // get the file list in the remote directory.
            String fileList = SshUtils.exec(remoteHost, remoteUser, keyLocation, "ls " + remoteLogLocation);
            String []files = fileList.split(" ");
            if (files.length > 0) {
                Arrays.sort(files);
            } else {
                logger.error("no log file found! task_id is: {}", message.lzTaskExecRecord.taskId);
                return null;
            }
            // prepare the remote log file.
            String remoteLogFileName = files[files.length - 1];
            String remoteLogFile = String.format("%s/%s", remoteLogLocation, remoteLogFileName);
            // prepare the local log file.
            localLogFile += String.format("tasklog/%d/%s/", lzRecord.taskType, lzRecord.taskId);
            new File(localLogFile).mkdirs();

            // fetch the remote log file to local directory.
            logger.info("local log directory is: {}", localLogFile);
            SshUtils.fileFetch(remoteHost, remoteUser, keyLocation, remoteLogFile, localLogFile);
            localLogLocation = localLogFile + remoteLogFileName;
        }

        // for debug.
        if (localLogLocation == null) {
            logger.error("log file location error!");
            throw new Exception("log file location error!");
        } else {
            logger.info("log file to parse: {}", localLogLocation);
        }
        return localLogLocation;
    }
}
