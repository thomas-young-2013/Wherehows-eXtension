package wherehows.common.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 5/9/17.
 */
public class FtpUtils {

    public static boolean uploadFile(String url, int port, String username, String password,
                                     String path, String filename, InputStream input) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.changeWorkingDirectory(path);
            ftp.storeFile(filename, input);

            input.close();
            ftp.logout();
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    public static List<String> getFileContent(String url, int port, String username, String password,
                                              String remotePath, String fileName) {
        List<String> res = new ArrayList<String>();
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            InputStream in;
            ftp.connect(url, port);
            ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return res;
            }

            ftp.changeWorkingDirectory(remotePath);
            in = ftp.retrieveFileStream(fileName);
            if (in != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String data;
                try {
                    while ((data = br.readLine()) != null) {
                        res.add(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ftp.logout();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
        return res;
    }

    public static boolean downloadFile(String url, int port, String username, String password,
                                       String remotePath, String fileName, String localPath) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.changeWorkingDirectory(remotePath);
            FTPFile[] fs = ftp.listFiles();
            System.out.println(fs.length);
            for (FTPFile ff : fs) {
                System.out.println(ff.getName());
                if (ff.getName().equals(fileName)) {
                    File localFile = new File(localPath + "/" + ff.getName());
                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
                    is.close();
                }
            }
            ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }

    public static void main(String[] args) throws Exception {
        String host;
        if (args.length != 2) {
            System.out.println("args number is not correct!");
            return;
        }
        if (!args[1].equalsIgnoreCase("private")) host = "localhost";
        else host = "localhost";
        String username = "username";
        String password = "password";
        int port = 21;

        if (args[0].equalsIgnoreCase("upload")) {
            FileInputStream in = new FileInputStream(new File("/root/.ssh/id_rsa.pub"));
            boolean res = FtpUtils.uploadFile(host, port, username,
                    password, "/admin/all/20170426/", "1.txt", in);
            System.out.println(res);
            return;
        }
        if (args[0].equalsIgnoreCase("scan")) {
            List<String> res = FtpUtils.getFileContent(host, port, username,
                    password, "/admin/all/20170426/", "hive_sql.sql");
            System.out.println(res);
            return;
        } else {
            boolean res = downloadFile(host, port, username,
                    password, "/admin/all/20170426/",
                    "hive_sql.sql", "/root");
            System.out.println(res);
        }
    }
}