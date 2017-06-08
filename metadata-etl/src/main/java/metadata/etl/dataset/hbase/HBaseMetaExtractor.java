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
import wherehows.common.Constant;
import wherehows.common.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


/**
 * Created by lake on 17-5-23.
 */
public class HBaseMetaExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetaExtractor.class);
    public Admin admin;
    private Configuration config;
    private Connection con;
    private FileWriter schemaFileWriter;
    private FileWriter sampleFileWriter;
    private Properties prop;


    private String hbaseMetaFile = null;
    private String hbaseSampleFile = null;

    private final int SAMPLE_DATA_ROW_NUM = 5;
    private final int SAMPLE_DATA_COLUMN_NUM = 10;

    private void init() throws IOException {

        config = HBaseConfiguration.create();

        String zookeeperQuorum = prop.getProperty(Constant.HBASE_ZOOKEEPER_QUORUM_KEY);
        String zkPropertyClientPort = prop.getProperty(Constant.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_KEY);
        String masterPort = prop.getProperty(Constant.HBASE_MASTER_PORT_KEY);
        String masterBindAddress = prop.getProperty(Constant.HBASE_MASTER_INFO_BIND_ADDRESS_KEY);
        String zkZnodeParent = prop.getProperty(Constant.HBASE_ZOOKEEPER_ZNODE_PARENT_KEY);

        this.hbaseMetaFile = prop.getProperty(Constant.HBASE_LOCAL_RAW_META_DATA_KEY);
        this.hbaseSampleFile = prop.getProperty(Constant.HBASE_LOCAL_SAMPLE_KEY);

        config.set("hbase.master.port", masterPort);
        config.set("hbase.master.info.bindAddress", masterBindAddress);

        config.set("zookeeper.znode.parent", zkZnodeParent);
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", zkPropertyClientPort);

        con = ConnectionFactory.createConnection(config);
        admin = con.getAdmin();

        initFiles();
        initWriters();

    }

    public HBaseMetaExtractor(Properties prop) throws IOException {
        this.prop = prop;
        init();
    }


    private void initWriters() throws IOException {

        schemaFileWriter = new FileWriter(this.hbaseMetaFile);
        sampleFileWriter = new FileWriter(this.hbaseSampleFile);
        //if file exist and clear its content
        schemaFileWriter.write("");
        sampleFileWriter.write("");
    }


    private void initFiles() throws IOException {
        if (this.hbaseSampleFile == null || this.hbaseMetaFile == null) {
            String message = Constant.HBASE_LOCAL_RAW_META_DATA_KEY + " or " +
                    Constant.HBASE_LOCAL_SAMPLE_KEY + " is null,please config them in database";
            throw new IOException(message);
        }

        createFileIfNotExist(this.hbaseMetaFile);
        createFileIfNotExist(this.hbaseSampleFile);

    }

    private void createFileIfNotExist(String path) throws IOException {
        LOG.info("create file path : "+path);
        File file = new File(path);
        if (!file.exists()) {
            LOG.info("file path : "+path+" not exist , create it");
            String[] cmds = {"touch", path};
            ProcessUtils.exec(cmds);
        }
    }

    public void startToExtractHBaseData() throws IOException {
        TableName[] allTables = this.getAllTables();
        for (TableName tableName : allTables) {
            LOG.info("Hbase table : "+tableName.getNameAsString());
            this.extractTableInfo(tableName);
        }

        this.dataFlush();
        this.close();
    }

    public TableName[] getAllTables() throws IOException {
        return admin.listTableNames();
    }


    public void extractTableInfo(TableName tableName) throws IOException {

        Map<String, Object> keyToMeta = getTableProperties(tableName);


        Table table = con.getTable(tableName);
        Scan scan = new Scan();
        
        scan.setBatch(10);
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
            sampleFileWriter.append("hbase:///" + tableName.getNameAsString() + "\u001a" + null + "\u001a" + "{\"sample\": " + sampleList.toString() + "}" + "\n");
        }

    }

    private Map<String, Object> getTableProperties(TableName table) throws IOException {
        Map<String, Object> properties = new HashMap<String, Object>();
        Map<String, Object> writeFile = new HashMap<String, Object>();

        HTableDescriptor descriptor = admin.getTableDescriptor(table);
        Long maxFileSize = descriptor.getMaxFileSize();
        Long msFlushSize = descriptor.getMemStoreFlushSize();
        int replicationNum = descriptor.getRegionReplication();
        String owner = descriptor.getOwnerString();
        int coprocessorsNum = descriptor.getCoprocessors().size();
        String families = getAllFamilies(descriptor);


        properties.put("maxfilesize", maxFileSize);
        properties.put("memstoreflushsize", msFlushSize);
        properties.put("replicationnum", replicationNum);
        properties.put("owner", owner);
        properties.put("coprocessorsNum", coprocessorsNum);
        if (coprocessorsNum != 0) {
            for (int i = 0; i < coprocessorsNum; i++) {
                properties.put("coprocessor" + i, descriptor.getCoprocessors().get(i));
            }
        }
        properties.put("families", families);
        properties.put("flushpolicyclassname", "" + descriptor.getFlushPolicyClassName());
        properties.put("regionsplitpolicyclassname", "" + descriptor.getRegionSplitPolicyClassName());


        writeFile.put("attributes", properties);
        writeFile.put("uri", "hbase:///" + table.getNameAsString());
        writeFile.put("name", "Result");
        writeFile.put("namespace", "com.leishen");
        writeFile.put("type", "record");
        return writeFile;
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
            if (columnCount < SAMPLE_DATA_COLUMN_NUM) {
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
            fields.add(new ColumnType(column, "bytes"));
        return fields;
    }

    private String getAllFamilies(HTableDescriptor descriptor) {

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
     * use Gson change it to json string,like {name : 'age' ,type : 'int'}
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
