package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.XmlParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 5/10/17.
 */
public class SparkSubmitLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(SparkSubmitLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String destPath = xmlParser.getExtProperty("extProperties/entry/destFilePath");
            String sql = xmlParser.getExtProperty("extProperties/entry/filterSQL");
            long flowExecId = Long.parseLong(xmlParser.getExtProperty("curRunDate"));
            String databaseName = xmlParser.getExtProperty("extProperties/entry/databaseName");

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error happened in collecting lineage record.");
        }
        return lineageRecords;
    }
}
