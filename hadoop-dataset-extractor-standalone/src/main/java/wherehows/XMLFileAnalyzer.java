package wherehows;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
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
        if (!fs.exists(path))
            LOG.error("XML File Path: "+path.toUri().getPath()+" is not exist in HDFS");
        else {
            try {
                LOG.info("start parse xml ,path is %s"+path.toUri().getPath());
                startParseXML(path);
                FileStatus status = fs.getFileStatus(path);
                String schemaString = getXMLSchema();
                LOG.info("xml file schemaString is "+schemaString);
                String storage = STORAGE_TYPE;
                String abstractPath = path.toUri().getPath();
                String codec = "xml.format";
                return new DatasetJsonRecord(schemaString, abstractPath, status.getModificationTime(), status.getOwner(), status.getGroup(),
                        status.getPermission().toString(), codec, storage, "");
            } catch (Exception e) {
               LOG.error("path : "+path.toUri().getPath()+" XML File format is wrong ");
            }

        }
        return null;
    }

    @Override
    public SampleDataRecord getSampleData(Path path) throws IOException {
        if (!fs.exists(path))
            LOG.error("XML File Path is not exist in HDFS");
        else {
            List<Object> displays = new ArrayList<Object>();
            try {
                LOG.info("start parse xml ,path is "+path.toUri().getPath());
                startParseXML(path);
                for (String key : keyToValues.keySet()) {
                    displays.add("{\"" + key + "\":" + "\"" + keyToValues.get(key) + "\"}");
                }
                System.out.println("sample data is "+displays.toString());
            } catch (Exception e) {
                LOG.error("path : %s,XML File format is wrong ", path.toUri().getPath());
            }

            SampleDataRecord sampleDataRecord = new SampleDataRecord(path.toUri().getPath(), displays);
            LOG.info("sampledatarecord get success ");
            return sampleDataRecord;
        }
        return null;
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
