package wherehows;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 4/17/17.
 */
public class HiveExportFileAnalyzer extends FileAnalyzer {
    private String delemiter;
    public HiveExportFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "plain";
        delemiter = ",";
    }

    @Override
    public DatasetJsonRecord getSchema(Path targetFilePath)
            throws IOException {
        String filePath = targetFilePath.toUri().getPath();
        System.out.println("[getSchema] HiveExportFile path : " + filePath);
        // give it a try.
        if (!filePath.contains("000000_0")) return null;
        // if (!filePath.equalsIgnoreCase("/project/T405/out/000000_0")) return null;

        InputStream inputStream = fs.open(targetFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        int columnNum = 0;
        while((str = bufferedReader.readLine()) != null) {
            columnNum = str.split(delemiter).length;
            System.out.println(String.format("the first column string is: %s", str));
            break;
        }
        // debug.
        System.out.println("the number of column is: " + columnNum);

        inputStream.close();
        bufferedReader.close();
        // if the number of column is zero, file format unmatched.
        if (columnNum == 1) return null;

        String codec = "plain.codec";
        String schemaString = "{\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"int\"}], \"name\": \"Result\", \"namespace\": \"com.tencent.thomas\", \"type\": \"record\"}";
        String storage = STORAGE_TYPE;
        String abstractPath = targetFilePath.toUri().getPath();

        System.out.println("current file is: " + filePath);
        FileStatus fstat = fs.getFileStatus(targetFilePath);
        DatasetJsonRecord datasetJsonRecord =
                new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
                        fstat.getPermission().toString(), codec, storage, "");
        return datasetJsonRecord;
    }

    @Override
    public SampleDataRecord getSampleData(Path targetFilePath)
            throws IOException {

        String filePath = targetFilePath.toUri().getPath();
        System.out.println("[get sample data] HiveExportFile path : " + filePath);
        // give it a try.
        if (!filePath.contains("000000_0")) return null;

        /*InputStream inputStream = fs.open(targetFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        int count = 0;*/
        List<Object> list = new ArrayList<Object>();
        // the format of each element is json.
        list.add("{\"name\": \"thomas\", \"age\": 22}");
        list.add("{\"name\": \"yuan\", \"age\": 22}");
        /*while((str = bufferedReader.readLine()) != null && count < 10) {
            list.add(str);
            count++;
        }
        System.out.println("the count is : " + count);*/
        System.out.println("the sample data is : " + list.toString());

        SampleDataRecord sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);
        return sampleDataRecord;
    }
}
