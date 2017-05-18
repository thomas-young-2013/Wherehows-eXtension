package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hadoop on 5/18/17.
 */
public class DateFormat {
    public static String transform(long timestamp) {
        Date date = new Date(timestamp);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(date);
    }

    public static String format(String date) {
        return date.endsWith(".0")?date.substring(0, date.length()-2):date;
    }

}
