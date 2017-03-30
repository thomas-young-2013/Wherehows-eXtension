package metadata.etl.lhotse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;

import java.util.ArrayList;
import java.util.List;

import static wherehows.common.Constant.LZ_LINEAGE_LOG_DEFAULT_DIR;

/**
 * Created by thomas on 3/28/17.
 */
public class LzLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(LzLineageExtractor.class);
    public static final String defaultLogLocation = "/usr/local/lhotse_runners/log/";

    /**
     * Reference: Get one job's lineage.
     * Process :
     * 1 get execution log from lhotse base server
     * 2 get hadoop job id from execution log
     * 3 get input, output from execution log and hadoop conf, normalize the path
     * 4 construct the Lineage Record
     *
     * @return one lhotse task's lineage
     */
    public static List<LineageRecord> extractLineage(LzExecMessage message)
            throws Exception {

        List<LineageRecord> jobLineage = new ArrayList<>();

        LzTaskExecRecord lzRecord = message.lzTaskExecRecord;
        String logLocation = message.prop.getProperty(LZ_LINEAGE_LOG_DEFAULT_DIR, defaultLogLocation);
        // the full path
        logLocation += String.format("logtasklog/%d/%s", lzRecord.taskType, lzRecord.taskId);




        return jobLineage;
    }

    /**
     * Extract and write to database
     * @param message
     * @throws Exception
     */
    public static void extract(LzExecMessage message)
            throws Exception {
        try{
            List<LineageRecord> result = extractLineage(message);
            for (LineageRecord lr : result) {
                message.databaseWriter.append(lr);
            }
            logger.info(String.format("%03d lineage records extracted from [%s]", result.size(), message.toString()));
            message.databaseWriter.flush();
        } catch (Exception e) {
            logger.error(String.format("Failed to extract lineage info from [%s].\n%s", message.toString(), e.getMessage()));
        }
    }
}
