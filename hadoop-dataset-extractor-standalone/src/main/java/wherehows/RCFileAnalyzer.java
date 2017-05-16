package wherehows;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lake on 17-5-10.
 */
public class RCFileAnalyzer extends FileAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(RCFileAnalyzer.class);
    private static String COLUMN_NUMBER_KEY = "hive.io.rcfile.column.number";

    public RCFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "rc";
    }

    @Override
    public DatasetJsonRecord getSchema(Path path) throws IOException {
        DatasetJsonRecord record = null;
        if (!fs.exists(path))
            LOG.error("file path : {} not in hdfs", path);
        else {
            try {
                RCFile.Reader reader = new RCFile.Reader(fs, path, fs.getConf());
                Map<Text, Text> meta = reader.getMetadata().getMetadata();
                /** rcfile column number */
                int columnNumber = Integer.parseInt(meta.get(new Text(COLUMN_NUMBER_KEY)).toString());
                FileStatus status = fs.getFileStatus(path);
                String schemaString = getRCFileSchema(columnNumber);
                String storage = STORAGE_TYPE;
                String abstractPath = path.toUri().getPath();
                String codec = "rc.codec";
                record = new DatasetJsonRecord(schemaString, abstractPath, status.getModificationTime(), status.getOwner(), status.getGroup(),
                        status.getPermission().toString(), codec, storage, "");
                LOG.info("rc file : {} schema is {}", path.toUri().getPath(), schemaString);
            } catch (Exception e) {
                LOG.error("path : {} content " + " is not RC File format content  ", path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }
        }

        return record;
    }

    @Override
    public SampleDataRecord getSampleData(Path path) throws IOException {
        SampleDataRecord sampleDataRecord = null;
        List<Object> sampleData = null;
        if (!fs.exists(path))
            LOG.error(" File Path: " + path.toUri().getPath() + " is not exist in HDFS");
        else {
            try {
                RCFile.Reader reader = new RCFile.Reader(fs, path, fs.getConf());
                sampleData = getSampleData(reader);
                sampleDataRecord = new SampleDataRecord(path.toUri().getPath(), sampleData);
            } catch (Exception e) {
                LOG.error("path : {} content " + " is not RC File format content  ", path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }
        }
        return sampleDataRecord;
    }

    private String getRCFileSchema(int columnNumber) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"fields\": [");
        for (int i = 0; i < columnNumber - 1; i++) {
            builder.append("{\"name\":");
            builder.append("\"col" + i + "\",");
            builder.append("\"type\":");
            builder.append("\"Writable\"},");
        }
        builder.append("{\"name\":");
        builder.append("\"col" + (columnNumber - 1) + "\",");
        builder.append("\"type\":");
        builder.append("\"Writable\"}]}");
        return builder.toString();
    }

    private List<Object> getSampleData(RCFile.Reader reader) throws Exception {
        List<Object> sampleData = new ArrayList<Object>();
        LongWritable rowID = new LongWritable(0);
        BytesRefArrayWritable cols = new BytesRefArrayWritable();
        while (reader.next(rowID)) {
            reader.getCurrentRow(cols);
            BytesRefWritable brw = null;
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            for (int i = 0; i < cols.size() - 1; i++) {
                brw = cols.get(i);
                builder.append("\"col" + i + "\":" + "\"" + Bytes.toString(brw.getData(), brw.getStart(),
                        brw.getLength()) + "\",");
            }
            brw = cols.get(cols.size() - 1);
            builder.append("\"col" + (cols.size() - 1) + "\":" + "\"" + Bytes.toString(brw.getData(), brw.getStart(),
                    brw.getLength()) + "\"}");
            sampleData.add(builder.toString());
        }
        return sampleData;
    }

}
