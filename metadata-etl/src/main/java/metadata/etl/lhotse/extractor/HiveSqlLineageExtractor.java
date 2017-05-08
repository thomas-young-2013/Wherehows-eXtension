package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import metadata.etl.utils.XmlParser;
import metadata.etl.utils.hiveparser.HiveSqlAnalyzer;
import metadata.etl.utils.hiveparser.HiveSqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.ProcessUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 5/8/17.
 */
public class HiveSqlLineageExtractor implements BaseLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(Hive2HdfsLineageExtractor.class);

    @Override
    public List<LineageRecord> getLineageRecord(String logLocation, LzExecMessage message,
                                                int defaultDatabaseId) {
        LzTaskExecRecord lzTaskExecRecord = message.lzTaskExecRecord;
        List<LineageRecord> lineageRecords = new ArrayList<>();
        try {
            logger.info("start to parse the log: {}", logLocation);
            XmlParser xmlParser = new XmlParser(logLocation);
            // get info from logs
            String sqlFilePath = xmlParser.getExtProperty("extProperties/entry/sql.file.name");
            long flowExecId = Long.parseLong(xmlParser.getExtProperty("curRunDate"));

            // read sql statements from sql file on ftp

        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineageRecords;
    }
}
