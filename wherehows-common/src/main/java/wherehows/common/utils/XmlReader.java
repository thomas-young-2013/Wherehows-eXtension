package wherehows.common.utils;

import java.io.File;
import java.util.List;

/**
 * Created by hadoop on 3/30/17.
 */
public class XmlReader {
    public String fileLocation;

    public XmlReader(String fileLocation) {
        this.fileLocation = fileLocation;
    }

    public String get(String key) {
        /*SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(new File(fileLocation));
        Element foo = doc.getRootElement();

        List<?> allChildren = foo.getChildren();*/
        return null;
    }
}
