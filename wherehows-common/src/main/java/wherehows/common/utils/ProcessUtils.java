package wherehows.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Created by thomas young on 4/20/17.
 */
public class ProcessUtils {
    protected static final Logger logger = LoggerFactory.getLogger("ProcessUtils.class");

    public static ArrayList<String> exec(String []cmds) {
        ArrayList<String> result = null;
        try {
            java.lang.ProcessBuilder pb = new java.lang.ProcessBuilder(cmds);
            pb.redirectErrorStream(true);
            Process process = pb.start();
            int pid = -1;
            if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
            /* get the PID on unix/linux systems */
                try {
                    Field f = process.getClass().getDeclaredField("pid");
                    f.setAccessible(true);
                    pid = f.getInt(process);
                } catch (Throwable e) {
                }
            }
            logger.info("executue command [PID=" + pid + "]: " + cmds);

            // wait until this process finished.
            int execResult = process.waitFor();

            // if the process failed, log the error and throw exception
            if (execResult > 0) {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String errString = "Process executed Error:\n";
                String line = "";
                while ((line = br.readLine()) != null)
                    errString = errString.concat(line).concat("\n");
                logger.error("*** Process  failed, status: " + execResult);
                logger.error(errString);
                throw new Exception("Process + " + pid + " failed");
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line = "";
                result = new ArrayList<String>();
                while ((line = br.readLine()) != null) {
                    result.add(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = null;
        }
        return result;
    }

}
