package metadata.etl.utils.hiveparser;

/**
 * Created by thomas young on 4/6/17.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSqlAnalyzer {
    private static Logger LOG = LoggerFactory.getLogger(HiveSqlAnalyzer.class);
    static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
        /**
         * Creates an ASTNode for the given token. The ASTNode is a wrapper
         * around antlr's CommonTree class that implements the Node interface.
         *
         * @param payload
         *            The token.
         * @return Object (which is actually an ASTNode) for the token.
         */
        @Override
        public Object create(Token payload) {
            return new ASTNode(payload);
        }
    };


    static String collectTableNamesNew(ASTNode input,
                                       List<String> isrcTableNames,
                                       List<String> idesTableNames) {


        if (input.getToken().getType() == HiveParser.TOK_ALTERTABLE) {
            int childCount = input.getChildCount();
            if(childCount == 2){
                ASTNode n0 = (ASTNode)input.getChild(0);
                ASTNode n1 = (ASTNode)input.getChild(1);
                String opt = n1.getToken().getText();
                int nnCount = n0.getChildCount();
                if(nnCount == 1){
                    String tableName = n0.getChild(0).getText();
                    idesTableNames.add(tableName);
                }else if(nnCount == 2){
                    String tableName = n0.getChild(1).getText();
                    idesTableNames.add(tableName);
                }

                if(opt.equals("TOK_ALTERTABLE_RENAME")){
                    return "RENAMETABLE";
                }else{
                    return "ALTERTABLE";
                }
            }
        }

        if (input.getToken().getType() == HiveParser.TOK_DROPTABLE) {
            int childCount = input.getChildCount();
            ASTNode n0 = (ASTNode)input.getChild(0);
            if(childCount == 1){
                int count = n0.getChildCount();
                if(count == 1){
                    String tbName = n0.getChild(0).getText();
                    idesTableNames.add(tbName);
                }else if(count == 2){
                    String tbName = n0.getChild(1).getText();
                    idesTableNames.add(tbName);
                }

            }
            return HiveSqlType.DROPTB;
        }

        if (input.getToken().getType() == HiveParser.TOK_DROPDATABASE) {
            int childCount = input.getChildCount();
            ASTNode n0 = (ASTNode)input.getChild(0);
            if(childCount == 1){
                String tbName = n0.getToken().getText();
                idesTableNames.add(tbName);
            }
            return HiveSqlType.DROPDB;
        }

        if (input.getToken().getType() == HiveParser.TOK_SWITCHDATABASE) {
            int childCount = input.getChildCount();
            ASTNode n0 = (ASTNode)input.getChild(0);
            if(childCount == 1){
                String tbName = n0.getToken().getText();
                idesTableNames.add(tbName);
            }
            return HiveSqlType.SWITCHDB;
        }

        if (input.getToken().getType() == HiveParser.TOK_CREATEDATABASE) {
            int childCount = input.getChildCount();
            ASTNode n0 = (ASTNode)input.getChild(0);
            if(childCount == 1){
                String tbName = n0.getToken().getText();
                idesTableNames.add(tbName);
            }
            return HiveSqlType.CREATEDB;
        }


        if (input.getToken().getType() == HiveParser.TOK_DELETE_FROM) {
            getTableNamesForUpdateDelete(isrcTableNames, input);
            return HiveSqlType.DELETE;
        }

        if (input.getToken().getType() == HiveParser.TOK_UPDATE_TABLE) {
            getTableNamesForUpdateDelete(isrcTableNames, input);
            return HiveSqlType.UPDATE;
        }

        if (input.getToken().getType() == HiveParser.TOK_CREATETABLE) {
            int childCount = input.getChildCount();
            ASTNode n0 = (ASTNode)input.getChild(0);
            if(childCount >0 && n0.getToken().getType()==HiveParser.TOK_TABNAME){
                int count = n0.getChildCount();
                if(count == 2){
                    ASTNode n00 = (ASTNode)n0.getChild(1);
                    String tbName = n00.getToken().getText();
                    idesTableNames.add(tbName);
                }else if(count == 1){
                    ASTNode n00 = (ASTNode)n0.getChild(0);
                    String tbName = n00.getToken().getText();
                    idesTableNames.add(tbName);
                }

            }
            if(childCount >= 2){
                ASTNode n2 = (ASTNode)input.getChild(1);

                if(n2.getToken().getType()==HiveParser.TOK_LIKETABLE){
                    if(n2.getChildCount()>=1){
                        ASTNode n21 = (ASTNode)n2.getChild(0);
                        if(n21.getToken().getType()	== HiveParser.TOK_TABNAME){
                            int count = n21.getChildCount();
                            if(count == 1){
                                ASTNode n211 = (ASTNode)n21.getChild(0);
                                String tbName = n211.getToken().getText();
                                isrcTableNames.add(tbName);
                            }else if(count == 2){
                                ASTNode n211 = (ASTNode)n21.getChild(1);
                                String tbName = n211.getToken().getText();
                                isrcTableNames.add(tbName);
                            }

                        }
                    }
                }
            }
            if(childCount >= 3){
                ASTNode n3 = (ASTNode)input.getChild(2);
                getSrcTablesReCur(isrcTableNames,(ASTNode)n3.getChild(0));
            }
            return HiveSqlType.CREATETB;
        }

        if (input.getToken().getType() == HiveParser.TOK_LOAD) {
            int childCount = input.getChildCount();
            if(childCount >= 1){
                ASTNode test0 = (ASTNode) (input.getChild(0));
                if(test0.getToken().getType() == HiveParser.StringLiteral ){
                    String dir = test0.getText();
                    isrcTableNames.add(dir);
                }
            }

            if(childCount >= 2){
                ASTNode test1 = (ASTNode) (input.getChild(1));
                if(test1.getToken().getType()==HiveParser.TOK_TAB){
                    ASTNode test11 =(ASTNode) test1.getChild(0);
                    if(test11!=null&&test11.getToken().getType() == HiveParser.TOK_TABNAME){
                        ASTNode test111 =(ASTNode) test11.getChild(0);
                        String table = test111.getText().toLowerCase();
                        String dbname = null;
                        if (test11.getChildCount() == 3) {
                            dbname = test11.getChild(2).getText().toLowerCase();
                        }else if (test11.getChildCount() == 2) {
                            dbname = test11.getChild(1).getText()
                                    .toLowerCase();
                            if (dbname.contains("partition")
                                    || dbname.contains("subpartition")) {
                                dbname = null;
                            }
                        }
                        idesTableNames.add(table);
                    }



                }
            }
            return HiveSqlType.LOAD;
        }

        if (input.getToken().getType() == HiveParser.TOK_QUERY && input.getChildCount() > 1) {
            ASTNode test0 = (ASTNode) (input.getChild(0));
            ASTNode test1 = (ASTNode) (input.getChild(1));
            if (test1 != null && test1.getToken().getType() == HiveParser.TOK_INSERT) {
                int cc = test1.getChildCount();
                for(int ii=0;ii<cc;ii++){
                    ASTNode test10 = (ASTNode) (test1.getChild(ii));
                    if (test10 != null && ((test10.getToken().getType() == HiveParser.TOK_DESTINATION )
                            ||(test10.getToken().getType() == HiveParser.TOK_INSERT_INTO)
                            ||(test10.getToken().getType() == HiveParser.TOK_WHERE) )) {

                        ASTNode test100 = (ASTNode) (test10.getChild(0));
                        if (test100 != null && test100.getToken().getType() == HiveParser.TOK_TAB) {

                            ASTNode test1000 = (ASTNode) (test100.getChild(0));
                            if (test1000 != null && test1000.getToken().getType() == HiveParser.TOK_TABNAME) {

                                int cCount = test1000.getChildCount();
                                String dest = "";
                                if(cCount>1){
                                    ASTNode test10000 = (ASTNode) (test1000.getChild(1));
                                    dest = test10000.getText().toLowerCase();
                                }else{
                                    ASTNode test10000 = (ASTNode) (test1000.getChild(0));
                                    dest = test10000.getText().toLowerCase();
                                }

                                String dbname = null;
                                if (test1000.getChildCount() == 3) {
                                    dbname = test1000.getChild(2).getText()
                                            .toLowerCase();
                                } else if (test1000.getChildCount() == 2) {
                                    dbname = test1000.getChild(1).getText()
                                            .toLowerCase();
                                    if (dbname.contains("partition")
                                            || dbname.contains("subpartition")) {
                                        dbname = null;
                                    }
                                }

                                idesTableNames.clear();
                                idesTableNames.add(dest);
                                isrcTableNames.clear();
                                isrcTableNames = getSrcTablesNew(test0,
                                        isrcTableNames);
                                return HiveSqlType.INSERT;
                            }
                        } else if (test100 != null && test100.getToken().getType() == HiveParser.TOK_DIR) {
                            isrcTableNames.clear();
                            isrcTableNames = getSrcTablesNew(test0, isrcTableNames);
                            int dirCount = test100.getChildCount();
                            for(int i=0;i<dirCount;i++ ){
                                ASTNode dirNode = (ASTNode) test100.getChild(i);
                                if(dirNode.getToken().getType() == HiveParser.StringLiteral ){
                                    String dir = dirNode.getText();
                                    idesTableNames.add(dir);
                                }
                            }

                            ASTNode test1000 =(ASTNode) test100.getChild(0);
                            if(test1000.getToken().getType() == HiveParser.StringLiteral){
                                return HiveSqlType.WRITEFILE;
                            }


                        }else if (test100 != null && test100.getToken().getType() == HiveParser.TOK_SUBQUERY_EXPR){
                            int cCount = test100.getChildCount();
                            for(int j=0;j<cCount;j++){
                                ASTNode test10000 =(ASTNode)test100.getChild(j);
                                if(test10000.getToken().getType() == HiveParser.TOK_QUERY){
                                    isrcTableNames = getSrcTablesNew((ASTNode)test10000.getChild(0),
                                            isrcTableNames);
                                }

                            }

                        }
                    }
                }
                return HiveSqlType.QUERY;

            }
        }

        return input.getToken().getText();
    }

    static List<String> getSrcTablesNew(ASTNode src,
                                        List<String> isrcTableNames) {
        getSrcTablesReCur(isrcTableNames, src);
        return isrcTableNames;
    }


    static void getSrcTablesReCur(List<String> re, ASTNode src) {
        if (src != null && src.getToken().getType() == HiveParser.TOK_FROM) {
            ASTNode src0 = (ASTNode) src.getChild(0);
            if (src0 != null && src0.getToken().getType() == HiveParser.TOK_TABREF) {
                ASTNode src00 = (ASTNode) src0.getChild(0);
                if (src00.getToken().getType() == HiveParser.TOK_TABNAME) {
                    String tname=null;
                    String dbname = null;
                    if(src00.getChildCount() == 1){
                        tname = src00.getChild(0).getText().toLowerCase();
                    }
                    else if (src00.getChildCount() == 3) {
                        tname = src00.getChild(3).getText().toLowerCase();
                        dbname = src00.getChild(2).getText().toLowerCase();
                    } else if (src00.getChildCount() == 2) {
                        tname = src00.getChild(1).getText().toLowerCase();
                        dbname = src00.getChild(0).getText().toLowerCase();
                        if (dbname.contains("partition")
                                || dbname.contains("subpartition")) {
                            dbname = null;
                        }
                    }
                    re.add(tname);
                }
            } else if (src0.getToken().getType() == HiveParser.TOK_JOIN
                    || src0.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
                    || src0.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
                    || src0.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
                    || src0.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
                resolveJoinTok(re, src0);
            } else if (src0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
                ASTNode src00 = (ASTNode) src0.getChild(0);
                ASTNode src000 = (ASTNode) src00.getChild(0);
                if (src000 != null
                        && src000.getToken().getType() == HiveParser.TOK_FROM) {
                    getSrcTablesReCur(re, src000);
                } else if (src00.getToken().getType() == HiveParser.TOK_UNIONALL) {
                    resolveUnionTok(re, src00);
                }
            }
        }
    }

    static void resolveJoinTok(List<String> re, ASTNode src0) {
        ASTNode src00 = (ASTNode) src0.getChild(0);
        if (src00 != null && src00.getToken().getType() == HiveParser.TOK_TABREF) {
            ASTNode src000 = (ASTNode) src00.getChild(0);
            if (src000.getToken().getType() == HiveParser.TOK_TABNAME) {
                String tname = src000.getChild(0).getText().toLowerCase();
                String dbname = null;
                if (src000.getChildCount() == 3) {
                    dbname = src000.getChild(2).getText().toLowerCase();
                } else if (src000.getChildCount() == 2) {
                    dbname = src000.getChild(1).getText().toLowerCase();
                    if (dbname.contains("partition")
                            || dbname.contains("subpartition")) {
                        dbname = null;
                    }
                }
                re.add(tname);
            }
        } else if (src00 != null
                && src00.getToken().getType() == HiveParser.TOK_SUBQUERY) {
            ASTNode src000 = (ASTNode) src00.getChild(0);
            ASTNode src0000 = (ASTNode) src000.getChild(0);
            if (src0000 != null
                    && src0000.getToken().getType() == HiveParser.TOK_FROM) {
                getSrcTablesReCur(re, src0000);
            } else if (src000.getToken().getType() == HiveParser.TOK_UNIONALL) {
                resolveUnionTok(re, src000);
            }
        } else if (src00.getToken().getType() == HiveParser.TOK_JOIN
                || src00.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
                || src00.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
                || src00.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
                || src00.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
            resolveJoinTok(re, src00);
        }
        ASTNode src01 = (ASTNode) src0.getChild(1);
        if (src01 != null
                && src01.getToken().getType() == HiveParser.TOK_TABREF) {
            ASTNode src010 = (ASTNode) src01.getChild(0);
            if (src010.getToken().getType() == HiveParser.TOK_TABNAME) {
                String tname = src010.getChild(0).getText().toLowerCase();
                String dbname = null;
                if (src010.getChildCount() == 3) {
                    dbname = src010.getChild(2).getText().toLowerCase();
                } else if (src010.getChildCount() == 2) {
                    dbname = src010.getChild(1).getText().toLowerCase();
                    if (dbname.contains("partition")
                            || dbname.contains("subpartition")) {
                        dbname = null;
                    }
                }
                re.add(tname);
            }
        } else if (src01 != null
                && src01.getToken().getType() == HiveParser.TOK_SUBQUERY) {
            ASTNode src010 = (ASTNode) src01.getChild(0);
            ASTNode src0100 = (ASTNode) src010.getChild(0);
            if (src0100 != null
                    && src0100.getToken().getType() == HiveParser.TOK_FROM) {
                getSrcTablesReCur(re, src0100);
            } else if (src010.getToken().getType() == HiveParser.TOK_UNIONALL) {
                resolveUnionTok(re, src010);
            }
        } else if (src01.getToken().getType() == HiveParser.TOK_JOIN
                || src01.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
                || src01.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
                || src01.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
                || src01.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
            resolveJoinTok(re, src01);
        }
    }

    static void resolveUnionTok(List<String> re, ASTNode src) {
        for (int i = 0; i < src.getChildCount(); i++) {
            ASTNode tmpast = (ASTNode) src.getChild(i);
            if (((ASTNode) tmpast.getChild(0)).getToken().getType() == HiveParser.TOK_FROM) {
                getSrcTablesReCur(re, (ASTNode) tmpast.getChild(0));
            } else if (tmpast.getToken().getType() == HiveParser.TOK_UNIONALL) {
                resolveUnionTok(re, tmpast);
            }
        }
    }

    public static void getTableNamesForUpdateDelete(
            List<String> isrcTableNames, ASTNode input) {
        if (input == null) {
            return;
        }
        if (input.getToken().getType() == HiveParser.TOK_TABNAME) {
            if (input.getChildCount() == 1) {
                isrcTableNames.add(input.getChild(0).getText());
                return;
            } else if (input.getChildCount() == 2) {
                isrcTableNames.add(input.getChild(0).getText());
                return;
            } else if (input.getChildCount() == 3) {
                isrcTableNames.add(input.getChild(0).getText());
                return;
            }
        } else {
            int childCount = input.getChildCount();
            for (int i = 0; i < childCount; i++) {
                getTableNamesForUpdateDelete(isrcTableNames,
                        (ASTNode) input.getChild(i));
            }
        }
    }

    public static ASTNode findRootNonNullToken(ASTNode tree) {
        if (tree == null) {
            return null;
        }
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }

    public static String analyzeSql(String sql,List<String> isrcTableNames ,List<String> idesTableNames){
        try {
            ParseDriver pd = new ParseDriver();
            ASTNode node = pd.parse(sql);
            node = findRootNonNullToken(node);
            String opType = collectTableNamesNew(node, isrcTableNames,idesTableNames);
            return opType;
        }  catch (Exception e) {
            LOG.warn("analyzeSql sql fail "+e);
        }
        return "UNKNOW";
    }

    public static void main(String[] args) throws Exception {
        String sql1 = "select * from stb1   where exists (select * from stb2 where stb2.c2 = t1.c1) ";
        String sql2 = "Select * from stb1 where age > 10 and area in (1,2)";
        String sql3 = "Select d.name,d.ip from (select * from zpc3 ) d";

        String sql7 = "SELECT id, value FROM (SELECT id, value FROM p1 UNION ALL  SELECT 4 AS id, 5 AS value FROM p1 limit 1) u";
        String sql8 = "select dd from(select id+1 dd from zpc) d";
        String sql9 = "select dd+1 from(select id+1 dd from zpc) d";
        String sql10 = "truncate table zpc";
        String sql11 = "drop table zpc";
        String sql15 = "alter table old_table_name RENAME TO new_table_name";
        String sql14 = "SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)";
        String sql18 = "CREATE TABLE  table1     (    column1 STRING COMMENT 'comment1',    column2 INT COMMENT 'comment2'        )";
        String sql19 = "ALTER TABLE events RENAME TO 3koobecaf";
        String sql20 = "ALTER TABLE invites ADD COLUMNS (new_col2 INT COMMENT 'a comment')";
        String sql22 = "select login.uid from login day_login left outer join (select uid from regusers where dt='20130101') day_regusers on day_login.uid=day_regusers.uid where day_login.dt='20130101' and day_regusers.uid is null";
        String sql23 = "select name from (select * from zpc left outer join def) d";
        String sql24 = "select t1.*,t2.value_data from t_hm_ru_03 t1 join ( select * from s_base_values where pt = '20110410000000' and value_id = 888 ) t2 on t1.brand_id = t2.value_id";
        String sql25 ="delete from t1";
        String sql26 ="update t1 set id = 123";
        String createPartition = "create table desTab(id INT, name STRING) partitioned by(academy STRING, class STRING) row format delimited fields terminated by ','";

        String createDb = "create database zl";
        String dropDb = "drop database zl";
        String createTB = "create table zpc (id int)";

        String DROPTB = "DROP table db.ZL";
        String useDB = "use zl ss";



        // have relation begin
        String insert = "insert into db1.sheld select * from db1.shel where a='aa'";
        String insert2 = "insert into table sheld select * from shel where a='aa'";

        String createLike = "CREATE  TABLE db1.desTab  LIKE db1.srcTab ";
        String createLike2 = "CREATE  TABLE desTab  LIKE srcTab ";

        String createSelect = "create table db1.desTab as select * from  db1.srcTab";
        String createSelect2 = "create table desTab as select * from  srcTab";

        String createSubQuery = "create table db1.desTab as select dd.a from (select *from srcTab,srcTab2) dd";
        String createSubQuery2 = "create table desTab as select dd.a from (select *from srcTab,srcTab2) dd";

        String insertOVERWRITESelect = "INSERT OVERWRITE TABLE db1.desTab select *  FROM db1.srcTab,db1.srcTab2";
        String insertOVERWRITESelect2 = "INSERT OVERWRITE TABLE desTab select *  FROM srcTab,srcTab2";

        String insertOVERWRITESubQuery = "INSERT OVERWRITE TABLE db1.desTab select dd.a from (select *from db1.srcTab,db1.srcTab2) dd";
        String insertOVERWRITESubQuery2 = "INSERT OVERWRITE TABLE desTab select dd.a from (select *from srcTab,srcTab2) dd";

        String insertIntoSelect = "insert INTO db1.desTab select * from srcTab,srcTab2";
        String insertIntoSelect2 = "insert INTO desTab select * from srcTab,srcTab2";

        String insertIntoSubSelect = "insert INTO table desTab select dd.a from (select *from srcTab,srcTab2) dd";
        String load = "LOAD DATA  INPATH 'hdfs://hdfsCluster/project/hsf_project_ranger/test' OVERWRITE INTO TABLE desTab";
        String load1 =" LOAD DATA INPATH '/user/myname/kv2.txt' OVERWRITE INTO TABLE desTab PARTITION (ds='2008-08-15')";
        String load2 = "load data inpath 'hdfs://hdfsCluster/project/hsf_project_ranger/test' overwrite into table shelde";
        String writeFile ="INSERT OVERWRITE DIRECTORY '/tmp/hdfs_out' SELECT a.* FROM srcTab,srcTab2";

        String alterTableColSql = "ALTER TABLE name1 ADD COLUMNS (col_name1 STRING,col_name2 STRING)";
        String alterTableName = "ALTER TABLE db.name RENAME TO db.new_name";
        String alterTableProperties = "ALTER TABLE table_name SET SERDEPROPERTIES ('field.delim' = ',')";
        // have relation end

        List<String> isrcTableNames = new ArrayList<String>();
        List<String> idesTableNames = new ArrayList<String>();
        String opType = HiveSqlAnalyzer.analyzeSql(sql22, isrcTableNames, idesTableNames);
        System.out.println(opType.equals(HiveSqlType.DROPDB));
        System.out.println(opType);
        System.out.println(isrcTableNames);
        System.out.println(idesTableNames);

    }

}
