package wherehows.common.utils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by thomas young on 4/12/17.
 */
public class SshUtils {
    public static String exec(String host, String user, String keyLocation, String command) {
        String result = "";
        Session session = null;
        ChannelExec openChannel = null;
        try {
            JSch jsch = new JSch();

            session = jsch.getSession(user, host);
            jsch.addIdentity(keyLocation);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            openChannel = (ChannelExec) session.openChannel("exec");
            openChannel.setCommand(command);
            openChannel.connect();

            InputStream in = openChannel.getInputStream();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in));
            String buf = null;
            while ((buf = reader.readLine()) != null) {
                result += " " + buf;
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = null;
        } finally {
            if (openChannel != null && !openChannel.isClosed()) {
                openChannel.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
        return result;
    }

    public static void fileFetch(String host, String user, String keyLocation, String sourceDir, String destDir) {
        JSch jsch = new JSch();
        Session session = null;
        try {
            // set up session
            session = jsch.getSession(user,host);
            // use private key instead of username/password
            session.setConfig(
                    "PreferredAuthentications",
                    "publickey,gssapi-with-mic,keyboard-interactive,password");
            jsch.addIdentity(keyLocation);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            // copy remote log file to localhost.
            ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            channelSftp.get(sourceDir, destDir);
            channelSftp.exit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.disconnect();
        }
    }
}