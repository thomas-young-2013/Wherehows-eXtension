package wherehows;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.Type;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lake on 17-5-9.
 */
public class ParquetFileAnalyzer extends FileAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(ParquetFileAnalyzer.class);

    public ParquetFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "parquet";
    }

    @Override
    public DatasetJsonRecord getSchema(Path path) throws IOException {
        DatasetJsonRecord record = null;
        if (!fs.exists(path))
            LOG.error("file path : {} not in hdfs", path);
        else {
            try {
                ParquetMetadata readFooter = ParquetFileReader.readFooter(fs.getConf(), path, ParquetMetadataConverter.NO_FILTER);
                Map<String, String> schema = readFooter.getFileMetaData().getKeyValueMetaData();
                String allFields = schema.get("org.apache.spark.sql.parquet.row.metadata");
                FileStatus status = fs.getFileStatus(path);
                String storage = STORAGE_TYPE;
                String abstractPath = path.toUri().getPath();
                String codec = "parquet.codec";
                record = new DatasetJsonRecord(allFields, abstractPath, status.getModificationTime(), status.getOwner(), status.getGroup(),
                        status.getPermission().toString(), codec, storage, "");
                LOG.info("parquetfileanalyzer parse path :{},schema is {}", path.toUri().getPath(), record.toCsvString());

            } catch (Exception e) {
                LOG.error("path : {} content " + " is not Parquet File format content  ", path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }
        }
        return record;

    }

    @Override
    public SampleDataRecord getSampleData(Path path) throws IOException {
        SampleDataRecord record = null;
        List<Object> sampleDatas = new ArrayList<Object>();
        if (!fs.exists(path))
            LOG.error("file path : {} not in hdfs", path);
        else {
            try {
                ParquetMetadata readFooter = ParquetFileReader.readFooter(fs.getConf(), path, ParquetMetadataConverter.NO_FILTER);
                MessageType schema = readFooter.getFileMetaData().getSchema();
                List<Type> columnInfos = schema.getFields();
                ParquetReader<Group> reader =
                        ParquetReader.builder(new GroupReadSupport(), path).
                                withConf(fs.getConf()).build();
                int count = 0;
                Group recordData = reader.read();

                while (count < 10 && recordData != null) {
                    int last = columnInfos.size() - 1;
                    StringBuilder builder = new StringBuilder();
                    builder.append("{");
                    for (int j = 0; j < columnInfos.size(); j++) {
                        if (j < columnInfos.size() - 1) {
                            String columnName = columnInfos.get(j).getName();
                            String value = recordData.getValueToString(j, 0);
                            builder.append("\"" + columnName + "\":\"" + value + "\",");
                        }
                    }
                    String columnName = columnInfos.get(last).getName();
                    String value = recordData.getValueToString(last, 0);
                    sampleDatas.add(builder.append("\"" + columnName + "\":\"" + value + "\"}").toString().replace("\\", "\\\\"));
                    count++;
                    recordData = reader.read();
                }
                record = new SampleDataRecord(path.toUri().getPath(), sampleDatas);
                LOG.info("parquet get data success");
            } catch (Exception e) {
                LOG.error("path : {} content " + " is not Parquet File format content  ", path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }
        }
        return record;
    }
}
