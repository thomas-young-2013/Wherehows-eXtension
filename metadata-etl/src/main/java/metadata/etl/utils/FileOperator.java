package metadata.etl.utils;

import java.io.File;
import java.util.Arrays;

/**
 * Created by hadoop on 4/12/17.
 */
public class FileOperator {
    public static String getOneLogFile(String dir) {
        String[] fileLists = new File(dir).list();
        Arrays.sort(fileLists);
        return fileLists[fileLists.length - 1];
    }
}
