package wherehows;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiahuilliu on 4/19/17.
 */
public class CSVFileAnalyzer extends FileAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(CSVFileAnalyzer.class);

    public CSVFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "csv";
    }

    public List<String[]> getLineTOData(Path path, int lineNo) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        LineNumberReader reader = new LineNumberReader(br);
        reader.setLineNumber(lineNo);
        String str = "";
        List<String[]> lls = new ArrayList<String[]>();
        if (lineNo == 1 && (str = reader.readLine()) != null) {
            lls.add(str.split(","));
            return lls;
        } else if (lineNo >= 2 && (str = reader.readLine()) != null) {
            while ((str = reader.readLine()) != null) {
                lls.add(str.split(","));
            }
            return lls;
        } else {
            System.out.println("This is a EMPTY File");
            return null;
        }
    }

    public DatasetJsonRecord getSchema(Path targetFilePath) throws IOException {
        DatasetJsonRecord datasetJsonRecord = null;
        try {
            StringBuilder JsonObjectList = new StringBuilder();
            List lsList = this.getLineTOData(targetFilePath, 1);
            String[] lsString = (String[]) lsList.get(0);
            for (String realName : lsString) {
                JsonObjectList.append("{\"name\": \"" + realName + "\", \"type\": \"string\"},"); //4.28
            }
            JsonObjectList.deleteCharAt(JsonObjectList.length() - 1);
            String schemaString = "{\"fields\":[" + JsonObjectList + "],\"name\": \"Result\", \"namespace\": \"com.tencent.thomas\", \"type\": \"record\"}";
            String codec = "csv.codec";
            String storage = STORAGE_TYPE;
            String abstractPath = targetFilePath.toUri().getPath();
            FileStatus fstat = fs.getFileLinkStatus(targetFilePath);
            datasetJsonRecord =
                    new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
                            fstat.getPermission().toString(), codec, storage, "");
            LOG.info("csv schma get success , it is {}", datasetJsonRecord.toCsvString());
        } catch (Exception e) {
            LOG.error("path : {} content " + " is not CSV File format content  ",targetFilePath.toUri().getPath());
            LOG.info(e.getStackTrace().toString());
        }

        return datasetJsonRecord;
    }

    public SampleDataRecord getSampleData(Path targetFilePath) throws IOException {
        SampleDataRecord sampleDataRecord = null;
        try {
            List lsList1 = this.getLineTOData(targetFilePath, 1);
            List lsList2 = this.getLineTOData(targetFilePath, 2);
            List<Object> list = new ArrayList<>();
            String[] lsString = (String[]) lsList1.get(0);
            StringBuilder lineSample = new StringBuilder();
            String[] lsString2;
            for (int i = 0; i <= 1; i++) {
                for (int j = 0; j < lsString.length; j++) {
                    lsString2 = (String[]) lsList2.get(i);
                    lineSample.append("\"" + lsString[j] + "\": \"" + lsString2[j] + "\",");
                }
                lineSample.deleteCharAt(lineSample.length() - 1); //4.28
                list.add("{" + lineSample + "}");
                lineSample.delete(0, lineSample.length());
            }
            System.out.println("The sample data is " + list.toString());
            sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);
        } catch (Exception e) {
            LOG.error("path : {} content " + " is not CSV File format content  ",targetFilePath.toUri().getPath());
            LOG.info(e.getStackTrace().toString());
        }

        return sampleDataRecord;
    }
}
