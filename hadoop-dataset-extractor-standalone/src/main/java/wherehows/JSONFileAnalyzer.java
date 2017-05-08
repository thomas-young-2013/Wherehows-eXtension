package wherehows;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by jiahuilliu on 5/1/17.
 */
public class JSONFileAnalyzer extends FileAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(JSONFileAnalyzer.class);

    public JSONFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "json";
    }

    private JSONObject getJsonObject(Path path) throws IOException, JSONException {
        String line = null;
        StringBuffer lineString = new StringBuffer();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        while ((line = br.readLine()) != null) {
            lineString.append(line);
        }
        JSONObject jsonObject = new JSONObject(lineString.toString());
        return jsonObject;
    }

    private boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isInt = pattern.matcher(str);
        if (!isInt.matches()) {
            return false;
        }
        return true;
    }

    private String[] json2Array(JSONObject jsonObject, String type) {
        String string = jsonObject.toString();
        string = string.replace("}", "").replace("{", "").replace("[", "").replace("]", "").replace("\"", "");
        String[] strings = string.split(",");

        if (type.equals("schema")) {
            String[] stringsKV = new String[strings.length];
            for (int i = 0; i < strings.length; i++) {
                if (strings[i].split(":").length < 3) {
                    if (this.isInteger(strings[i].split(":")[1])) {
                        stringsKV[i] = "$" + strings[i].split(":")[0];
                    } else {
                        stringsKV[i] = strings[i].split(":")[0];
                    }
                } else if (strings[i].split(":").length >= 3) {
                    int i2 = i;
                    stringsKV[i] = strings[i2].split(":")[0] + "_" + strings[i].split(":")[1];
                }
            }
            return stringsKV;
        } /*else if (type.equals("sample")) {
            String[] stringsKV = new String[strings.length];
            for (int i = 0; i < strings.length; i++) {
                stringsKV[i] = strings[i].split(":")[1];
            }
            return stringsKV;
        }*/ else {
            return null;
        }
    }

    @Override
    public DatasetJsonRecord getSchema(Path targetFilePath) throws IOException {
        StringBuilder JsonObjectList = new StringBuilder();
        DatasetJsonRecord datasetJsonRecord = null;
        try {
            for (String realName : this.json2Array(getJsonObject(targetFilePath), "schema")) {
                if (realName.charAt(0) == '$') {
                    JsonObjectList.append("{\"name\": \"" + realName.substring(1, realName.length()) + "\", \"type\": \"int\"},");
                } else {
                    JsonObjectList.append("{\"name\": \"" + realName + "\", \"type\": \"string\"},");
                }
            }
            JsonObjectList.deleteCharAt(JsonObjectList.length() - 1);
            String schemaString = "{\"fields\":[" + JsonObjectList + "],\"name\": \"Result\", \"namespace\": \"com.tencent.thomas\", \"type\": \"record\"}";
            String codec = "json.codec";
            String storage = STORAGE_TYPE;
            String abstractPath = targetFilePath.toUri().getPath();
            FileStatus fstat = fs.getFileLinkStatus(targetFilePath);

            datasetJsonRecord =
                    new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
                            fstat.getPermission().toString(), codec, storage, "");
        } catch (Exception e) {
            LOG.error("path : {} content " + " is not JSON File format content  ",targetFilePath.toUri().getPath());
            LOG.info(e.getStackTrace().toString());
        }

        return datasetJsonRecord;
    }

    @Override
    public SampleDataRecord getSampleData(Path targetFilePath) throws IOException {
       /* try {
            JSONArray jsonArray = this.getJsonObject(targetFilePath).getJSONArray("");
            String sampleDate = null;
            for (int i = 0; i < jsonArray.length(); i++) {
                String clo1 = jsonArray.getJSONObject(i).getString("");
                String clo2 = jsonArray.getJSONObject(i).getString("");
                String clo3 = jsonArray.getJSONObject(i).getString("");
                JSONArray clo4 = jsonArray.getJSONObject(i).getJSONArray("");
            }
            List<Object>list=new ArrayList<>();
            list.add(jsonArray.toString());
            System.out.println(""+list.toString());
            SampleDataRecord sampleDataRecord=new SampleDataRecord(targetFilePath.toUri().getPath(),list);
            return  sampleDataRecord;
        }catch (JSONException e){
            e.printStackTrace();
            return null;
        }*/

        List<Object> list = new ArrayList<>();
        SampleDataRecord sampleDataRecord = null;
        try {
            list.add(this.getJsonObject(targetFilePath).toString());
            System.out.println("The sample data is " + list.toString());
            sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);

        } catch (Exception e) {
            LOG.error("path : {} content " + " is not JSON File format content  ",targetFilePath.toUri().getPath());
            LOG.info(e.getStackTrace().toString());

        }
        return sampleDataRecord;
    }
}

