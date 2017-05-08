package wherehows;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lake on 17-5-6.
 */
public class SequenceFileAnalyzer extends FileAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(SequenceFileAnalyzer.class);

    public SequenceFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "sequence";

    }

    @Override
    public DatasetJsonRecord getSchema(Path path) throws IOException {
        DatasetJsonRecord record = null;
        if (!fs.exists(path))
            LOG.error("sequencefileanalyzer file : " + path.toUri().getPath() + " is not exist on hdfs");
        else {
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(path));
                String keyName = "Key";
                String keyType = getWritableType(reader.getKeyClassName());
                String valueName = "Value";
                String valueType = getWritableType(reader.getValueClassName());
                FileStatus status = fs.getFileStatus(path);
                String storage = STORAGE_TYPE;
                String abstractPath = path.toUri().getPath();
                String codec = "sequence.codec";
                String schemaString = "{\"fields\": [{\"name\": \"" + keyName + "\", \"type\": \""+keyType+"\"}, {\"name\": \"" + valueName + "\", \"type\": \""+valueType+"\"}], \"name\": \"Result\", \"namespace\": \"com.tencent.lake\", \"type\": \"record\"}";
                record = new DatasetJsonRecord(schemaString, abstractPath, status.getModificationTime(), status.getOwner(), status.getGroup(),
                        status.getPermission().toString(), codec, storage, "");
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }

        }
        return record;
    }

    @Override
    public SampleDataRecord getSampleData(Path path) throws IOException {
        SampleDataRecord dataRecord = null;
        if (!fs.exists(path))
            LOG.error("sequence file : " + path.toUri().getPath() + " is not exist on hdfs");
        else {
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(path));
                List<Object> sampleValues = new ArrayList<Object>();
                Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
                Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
                int count = 0;
                String keyName = "Key";
                String valueName = "Value";
                while (reader.next(key, value) && count < 12) {
                    sampleValues.add("{\"" + keyName + "\": \"" + key + "\", \"" + valueName + "\": \"" + value + "\"}");
                    count++;
                }
                dataRecord = new SampleDataRecord(path.toUri().getPath(), sampleValues);
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }
        }
        return dataRecord;

    }


    private String getWritableType(String name){
        return name.substring(name.lastIndexOf(".")+1);
    }
}
