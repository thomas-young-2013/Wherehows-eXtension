package wherehows.common.utils;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by thomas young on 3/31/17.
 */
public class XmlParser {
    private Logger logger = LoggerFactory.getLogger(XmlParser.class);

    public String logLocation;
    public Document doc;

    public XmlParser(String path) {
        logLocation = path;
        SAXBuilder builder = new SAXBuilder();
        try {
            doc = builder.build(new File(logLocation));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
            doc = null;
        }
    }

    public static void main(String []args) {
        try {
            XmlParser xmlParser = new XmlParser("/home/hadoop/Desktop/20170411000300.xml");
            // XmlParser xmlParser = new XmlParser("/home/hadoop/Desktop/20170327000000.xml");
            System.out.println(xmlParser.getExtProperty("extProperties/entry/filterSQL"));
            System.out.println(xmlParser.getExtProperty("extProperties/entry/destFilePath"));
            System.out.println(xmlParser.getExtProperty("curRunDate"));
            System.out.println(xmlParser.getExtProperty("extProperties/entry/databaseName"));
            // System.out.println(xmlParser.getExtProperty("globalParameters/entry/cdh3_hadoop"));

            // System.out.println(xmlParser.getExtProperty("sourceServers/com.tencent.teg.dc.lhotse.newrunner.ServerRuntime/password"));
            // System.out.println(xmlParser.getExtProperty("pollingSeconds"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getExtProperty(String key) {
        if (doc == null) return null;
        try {
            String []parts = key.split("/");
            int partSize = parts.length;
            Element element = doc.getRootElement();
            for (int i=0; i<parts.length-1; i++) {
                if (parts[i].equals("globalParameters") || parts[i].equals("extProperties")) {
                    element = element.getChild(parts[i]);
                    List<Element> elements = element.getChildren();
                    for (Element element1: elements) {
                        List<Element> elements1 = element1.getChildren();
                        if (elements1.size() != 2) logger.error("error: size != 2");
                        if (elements1.get(0).getText().equals(parts[partSize - 1])) {
                            return elements1.get(1).getText();
                        }
                    }
                    return null;
                } else {
                    element = element.getChild(parts[i]);
                }
            }
            List<Element> elements = element.getChildren();
            if (elements.size() == 2 && elements.get(0).getText().equals(parts[partSize-1])) {
                return elements.get(1).getText();
            } else {
                return element.getChild(parts[partSize-1]).getText();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public String getExtProperty2(String key) {
        if (doc == null) return null;
        try {
            String []parts = key.split("/");
            int partSize = parts.length;
            Element element = doc.getRootElement();
            /*for (int i=0; i<parts.length-1; i++) {
                if (parts[i].equals("globalParameters") || parts[i].equals("extProperties")) {
                    element = element.getChild(parts[i]);
                    List<Element> elements = element.getChildren();
                    for (Element element1: elements) {
                        List<Element> elements1 = element1.getChildren();
                        if (elements1.size() != 2) logger.error("error: size != 2");
                        if (elements1.get(0).getText().equals(parts[partSize - 1])) {
                            return elements1.get(1).getText();
                        }
                    }
                    return null;
                } else {
                    element = element.getChild(parts[i]);
                }
            }*/
            element=element.getChild(parts[partSize-2]);
            List<Element> elements = element.getChildren();
            if (elements.size() == 4 && elements.get(0).getText().equals(parts[partSize-1])) {
                return elements.get(1).getText();
            } else {
                return element.getChild(parts[partSize-1]).getText();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
