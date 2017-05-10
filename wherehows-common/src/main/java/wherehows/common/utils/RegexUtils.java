package wherehows.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by thomas young on 5/10/17.
 */
public class RegexUtils {
    public static boolean isPath(String str) {
        Pattern typePattern = Pattern.compile("(/\\w+)+");
        Matcher typeMatcher = typePattern.matcher(str);
        if (typeMatcher.find()) return true;
        return false;
    }
}
