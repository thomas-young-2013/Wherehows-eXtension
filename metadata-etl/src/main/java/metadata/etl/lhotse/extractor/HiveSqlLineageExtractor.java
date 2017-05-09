package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzExecMessage;
import metadata.etl.lhotse.LzTaskExecRecord;
import metadata.etl.utils.XmlParser;
import metadata.etl.utils.hiveparser.HiveSqlAnalyzer;
import metadata.etl.utils.hiveparser.HiveSqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.FtpUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 5/8/17.
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
            // split the file path
            int last_index = sqlFilePath.lastIndexOf("/");
            String sqlPath = sqlFilePath.substring(0, last_index);
            String sqlFileName = sqlFilePath.substring(last_index);

            String ftpHost = message.prop.getProperty(Constant.FTP_HOST_KEY);
            int port = Integer.parseInt(message.prop.getProperty(Constant.FTP_PORT));
            String userName = message.prop.getProperty(Constant.FTP_USERNAME_KEY);
            String password = message.prop.getProperty(Constant.FTP_PASSWORD_KEY);

            // read sql statements from sql file on ftp
            List<String> sqls = FtpUtils.getFileContent(ftpHost, port, userName, password,
                    sqlPath, sqlFileName);

            for (String sql: sqls) {
                List<String> isrcTableNames = new ArrayList<String>();
                List<String> idesTableNames = new ArrayList<String>();
                String opType = HiveSqlAnalyzer.analyzeSql(sql, isrcTableNames, idesTableNames);
                if (opType.equals(HiveSqlType.QUERY)) {
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineageRecords;
    }
}
