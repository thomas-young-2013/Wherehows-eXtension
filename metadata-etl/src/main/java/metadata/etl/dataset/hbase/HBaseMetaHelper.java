package metadata.etl.dataset.hbase;

/**
 * Created by lakeshen on 2017/5/23.
 */


import com.google.gson.Gson;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import wherehows.common.utils.ProcessUtils;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by lake on 17-5-23.
 */
public class HBaseMetaHelper {
    public Admin admin;
    private Configuration config;
    private Connection con;
    private FileWriter schemaFileWriter;
    private FileWriter sampleFileWriter;
    private final String META_DIR = "/usr/tmp/hbase-data/";
    private final String HBASE_META = "hbase_meta";
    private final String HBASE_SAMPLE = "hbase_sample";
    private final int SAMPLE_DATA_ROW_NUM = 5;

    private void init() throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.141.91.83,10.141.111.247,10.141.116.103");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master.port", "60000");
        config.set("hbase.master.info.bindAddress", "10.141.68.47");
        config.set("hbase.master.info.port", "60010");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        con = ConnectionFactory.createConnection(config);
        admin = con.getAdmin();

        initFiles();
        initWriters();

    }

    public HBaseMetaHelper() throws IOException {
        init();
    }

    private void initWriters() throws IOException {
        schemaFileWriter = new FileWriter(META_DIR + HBASE_META);
        sampleFileWriter = new FileWriter(META_DIR + HBASE_SAMPLE);
    }



    private void initFiles() throws IOException {
        createFileIfNotExist(META_DIR + HBASE_META);
        createFileIfNotExist(META_DIR + HBASE_SAMPLE);
        setFilesPermisstion();
    }

    private void createFileIfNotExist(String path) throws IOException {
        File metaDir = new File(META_DIR);
        File file = new File(path);

        if (file.exists()) {
            String[] cmds = {"rm", path};
            ProcessUtils.exec(cmds);
        }

        if (!metaDir.exists()) {
            String[] cmds = {"mkdir", META_DIR};
            ProcessUtils.exec(cmds);
        }

        String[] cmds = {"touch", path};
        ProcessUtils.exec(cmds);
    }

    private void setFilesPermisstion() throws IOException {
        String[] cmd = {"chmod", "777", "-R", META_DIR};
        ProcessUtils.exec(cmd);
    }


    public TableName[] getAllTables() throws IOException {
        return admin.listTableNames();
    }


    public void extractTableInfo(TableName tableName) throws IOException {

        Map<String, Object> keyToMeta = getTableMetaData(tableName);

        FilterList filterList = getSampleFilter();
        Table table = con.getTable(tableName);
        Scan scan = new Scan();
        scan.setMaxResultSize(10);
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();

        List<ColumnType> cts = new ArrayList<ColumnType>();
        if (result == null) {
            keyToMeta.put("fields", cts);
            String realJsonSchema = mapToJson(keyToMeta);
            schemaFileWriter.append(realJsonSchema + "\n");

        } else {
            List<String> displayColumns = getColumnsWithFirstRow(result);
            cts = getJsonFields(displayColumns);
            keyToMeta.put("fields", cts);
            String realJsonSchema = mapToJson(keyToMeta);
            schemaFileWriter.append(realJsonSchema + "\n");
            List<Object> sampleList = getSampleData(scanner, cts, result);
            sampleFileWriter.append("/hbase/" + tableName.getNameAsString() + "\u001a" + null + "\u001a" + "{\"sample\": " + sampleList.toString() + "}" + "\n");
        }

    }

    private Map<String, Object> getTableMetaData(TableName table) throws IOException {
        Map<String, Object> properties = new HashMap<String, Object>();
        Map<String, Object> writeFile = new HashMap<String, Object>();

        HTableDescriptor descriptor = admin.getTableDescriptor(table);
        Long maxFileSize = descriptor.getMaxFileSize();
        Long msFlushSize = descriptor.getMemStoreFlushSize();
        int replicationNum = descriptor.getRegionReplication();
        String owner = descriptor.getOwnerString();
        int coprocessorsNum = descriptor.getCoprocessors().size();
        String families = getAllColumnFamily(descriptor);


        properties.put("maxfilesize", maxFileSize);
        properties.put("memstoreflushsize", msFlushSize);
        properties.put("replicationnum", replicationNum);
        properties.put("owner", owner);
        properties.put("coprocessorsNum", coprocessorsNum);
        properties.put("families", families);
        properties.put("flushpolicyclassname", descriptor.getFlushPolicyClassName());
        properties.put("regionsplitpolicyclassname", descriptor.getRegionSplitPolicyClassName());


        writeFile.put("attributes", properties);
        writeFile.put("uri", "/hbase/" + table.getNameAsString());
        writeFile.put("name", "Result");
        writeFile.put("namespace", "com.leishen");
        writeFile.put("type", "record");
        return writeFile;
    }

    private FilterList getSampleFilter() {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter pageFilter = new PageFilter(20);
        Filter columnFilter = new ColumnPaginationFilter(1, 1);
        filterList.addFilter(pageFilter);
        filterList.addFilter(columnFilter);
        return filterList;
    }

    private List<Object> getSampleData(ResultScanner scanner, List<ColumnType> cts, Result firstResult) throws IOException {

        int totalRows = 0;
        Result result = firstResult;
        List<Object> sampleList = new ArrayList<Object>();
        while (result != null && totalRows < SAMPLE_DATA_ROW_NUM) {

            Map<String, Object> columnToValue = new HashMap<String, Object>();
            for (ColumnType type : cts) {
                String strFam = type.getName().split(":")[0];
                String strQua = type.getName().split(":")[1];
                byte[] family = Bytes.toBytes(strFam);
                byte[] qualier = Bytes.toBytes(strQua);
                if (result.containsColumn(family, qualier)) {
                    String value = Bytes.toString(result.getValue(family, qualier));
                    columnToValue.put(strFam + ":" + strQua, value);
                } else {
                    columnToValue.put(strFam + ":" + strQua, "null");
                }
            }
            sampleList.add(mapToJson(columnToValue));
            totalRows++;
            result = scanner.next();
        }

        return sampleList;
    }


    public String changeListToStr(List<String> values) {
        StringBuilder builder = new StringBuilder();
        for (String value : values) {
            builder.append(value);
            builder.append(",");
        }
        return builder.toString().substring(0, builder.length() - 1);
    }

    public void dataFlush() throws IOException {
        schemaFileWriter.flush();
        sampleFileWriter.flush();
    }

    public void close() throws IOException {
        schemaFileWriter.close();
        sampleFileWriter.close();
        con.close();
        admin.close();
    }

    private List<String> getColumnsWithFirstRow(Result result) {
        List<String> columns = new ArrayList<String>();
        int columnCount = 0;
        for (Cell value : result.listCells()) {
            if (columnCount < 10) {
                String family = Bytes.toString(value.getFamily());
                String qualifier = Bytes.toString(value.getQualifier());
                String column = family + ":" + qualifier;
                columns.add(column);
                columnCount++;
            }
        }
        return columns;
    }

    private List<ColumnType> getJsonFields(List<String> columns) {
        List<ColumnType> fields = new ArrayList<ColumnType>();
        for (String column : columns)
            fields.add(new ColumnType(column, "byte"));
        return fields;
    }

    private String getAllColumnFamily(HTableDescriptor descriptor) {

        if (descriptor.getFamilies().size() == 0) {
            return "";
        } else {
            StringBuilder builder = new StringBuilder();
            for (HColumnDescriptor family : descriptor.getFamilies()) {
                builder.append(family.getNameAsString());
                builder.append(",");
            }
            return builder.toString().substring(0, builder.length() - 1);
        }

    }

    private String mapToJson(Map<String, Object> keyValues) {
        Gson gson = new Gson();
        return gson.toJson(keyValues);
    }

    /**
     * ------------------------------------------------------------------------------
     */
    private class ColumnType {
        private String name;
        private String type;

        public ColumnType(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return name + ":" + type;
        }
    }
}
