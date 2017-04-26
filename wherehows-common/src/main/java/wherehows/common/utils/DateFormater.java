package wherehows.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by thomas young on 4/12/17.
 */
public class DateFormater {
    public static String transform(long timestamp) {
        Date date = new Date(timestamp);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(date);
    }
    public static int getInt(String dateStr) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return (int) (sdf.parse(dateStr).getTime()/1000);
    }
}
