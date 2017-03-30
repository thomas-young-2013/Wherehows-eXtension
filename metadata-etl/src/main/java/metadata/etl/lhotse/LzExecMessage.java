package metadata.etl.lhotse;

import wherehows.common.writers.DatabaseWriter;
import java.sql.Connection;
import java.util.Properties;

/**
 * Created by hadoop on 3/30/17.
 */
public class LzExecMessage {
    public LzTaskExecRecord lzTaskExecRecord;
    public Properties prop;


    public DatabaseWriter databaseWriter;
    public Connection connection;

    public LzExecMessage(LzTaskExecRecord lzTaskExecRecord, Properties prop) {
        this.lzTaskExecRecord = lzTaskExecRecord;
        this.prop = prop;
    }
}
