package wherehows;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.Element;

import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.*;
import java.util.*;

/**
 * Created by lake on 17-5-1.
 */
public class XMLFileAnalyzer extends FileAnalyzer {

    private static Logger LOG = LoggerFactory.getLogger(XMLFileAnalyzer.class);
    /**
     * xml key and values
     */
    private Map<String, String> keyToValues = new LinkedHashMap<String, String>();

    public XMLFileAnalyzer(FileSystem fs) {
        super(fs);
        LOG.info("Start init XMLFileAnalyzer");
        STORAGE_TYPE = "xml";
    }

    @Override
    public DatasetJsonRecord getSchema(Path path) throws IOException {
        DatasetJsonRecord record = null;
        if (!fs.exists(path))
            LOG.error(" File Path: " + path.toUri().getPath() + " is not exist in HDFS");
        else {
            try {
                LOG.info("xmlfileanalyzer start parse xml schema, path is {}" , path.toUri().getPath());
                startParseXML(path);
                FileStatus status = fs.getFileStatus(path);
                // replace "\" to  "\\"
                String schemaString = getXMLSchema().replace("\\","\\"+"\\");
                LOG.info("xml file schemaString is {} " , schemaString);
                String storage = STORAGE_TYPE;
                String abstractPath = path.toUri().getPath();
                String codec = "xml.codec";
                record = new DatasetJsonRecord(schemaString, abstractPath, status.getModificationTime(), status.getOwner(), status.getGroup(),
                        status.getPermission().toString(), codec, storage, "");
            } catch (Exception e) {
                LOG.error("path : {} content " + " is not XML File format content  ",path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }

        }
        return record;
    }

    @Override
    public SampleDataRecord getSampleData(Path path) throws IOException {
        SampleDataRecord sampleDataRecord = null;
        if (!fs.exists(path))
            LOG.error(" File Path: " + path.toUri().getPath() + " is not exist in HDFS");
        else {
            List<Object> displays = new ArrayList<Object>();
            try {
                LOG.info("xmlfileanalyzer start parse xml sampledata ,path is {} " , path.toUri().getPath());
                startParseXML(path);
                int count = 0;
                for (String key : keyToValues.keySet()) {
                    displays.add(("{\"" + key + "\":" + "\"" + keyToValues.get(key) + "\"}").replaceAll("\"", "\\\""));
                    if(count > 20)
                        break;
                    count ++;
                }
                sampleDataRecord = new SampleDataRecord(path.toUri().getPath(), displays);
               // LOG.info("xml sampledata is {}",sampleDataRecord.toCsvString());
            } catch (Exception e) {
                LOG.error("path : {} content " + " is not XML File format content  ",path.toUri().getPath());
                LOG.info(e.getStackTrace().toString());
            }
        }
        return sampleDataRecord;
    }


    private String getXMLContent(Path path) throws IOException {
        String line;
        InputStream stream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        StringBuilder builder = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }
        return builder.toString();
    }

    private void startParseXML(Path xmlPath) throws Exception {
        String xml = getXMLContent(xmlPath);
        SAXReader reader = new SAXReader();
        Document doc = reader.read(new ByteArrayInputStream(xml.getBytes("UTF-8")));
        Element rooElem = doc.getRootElement();
        getChildNodes(rooElem, "");
    }


    private void getChildNodes(Element node, String parent) {
        String nodeName = getNodename(node, parent);
        if (!node.getTextTrim().equals(""))
            keyToValues.put(nodeName, node.getTextTrim());

        List<Element> listElement = node.elements();
        for (Element e : listElement) {
            if (!parent.equals(""))
                getChildNodes(e, parent + "." + node.getName());
            else
                getChildNodes(e, node.getName());
        }
    }

    private String getNodename(Element node, String parent) {
        String nodeName = "";
        if (!parent.equals(""))
            nodeName = parent + "." + node.getName();
        else
            nodeName = node.getName();
        return nodeName;
    }

    private String getXMLSchema() {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        builder.append("{\"fields\": [");
        for (String key : keyToValues.keySet()) {
            if (keyToValues.size() == 1) {
                append(builder, key);
                builder.append("}");
            } else {
                if (count < keyToValues.size() - 1) {
                    append(builder, key);
                    builder.append(",");
                } else {
                    append(builder, key);
                }
            }
            count++;
        }
        builder.append("],");
        builder.append("\"name\":" + "\"Result\"," + "\"namespace\":" + "\"com.tencent.lake\"," + "\"type\":" + "\"record\"}");
        return builder.toString();
    }


    private void append(StringBuilder builder, String key) {
        builder.append("{");
        builder.append("\"name\":");
        builder.append("\"" + key + "\",");
        builder.append("\"type\":");
        builder.append("\"string\"");
        builder.append("}");
    }

}
