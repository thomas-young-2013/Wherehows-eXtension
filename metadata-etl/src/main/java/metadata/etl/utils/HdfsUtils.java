package metadata.etl.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 6/7/17.
 */
public class HdfsUtils {
    private static FileSystem fs;
    static {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("config/hdfs-site.xml"));
            conf.addResource(new Path("config/core-site.xml"));
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            fs = null;
        }
    }

    public static boolean exists(String path) throws Exception {
        try {
            Path file = new Path(path);
            return fs.exists(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean isFile(String path) throws Exception {
        try {
            Path file = new Path(path);
            return fs.isFile(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean isDirectory(String path) throws Exception {
        try {
            Path file = new Path(path);
            return fs.isDirectory(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static List<String> listFiles(String path) {
        List<String> result = new ArrayList<String>();
        try {
            FileStatus[] status = fs.listStatus(new Path(path));
            for (FileStatus file : status) {
                if (file.getPath().getName().startsWith("_") || file.isDirectory()) {
                    continue;
                }
                result.add(file.getPath().getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String []args) {
        String path = args[0];
        try {
            System.out.println(HdfsUtils.exists(path));
            System.out.println(HdfsUtils.isFile(path));
            System.out.println(HdfsUtils.isDirectory(path));
            System.out.println(HdfsUtils.listFiles(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
