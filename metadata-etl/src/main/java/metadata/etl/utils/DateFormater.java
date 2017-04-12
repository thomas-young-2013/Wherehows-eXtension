package metadata.etl.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hadoop on 4/12/17.
 */
public class DateFormater {
    public static String transform(long timestamp) {
        Date date = new Date(timestamp);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(date);
    }
}
