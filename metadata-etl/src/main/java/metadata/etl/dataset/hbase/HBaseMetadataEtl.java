package metadata.etl.dataset.hbase;

import metadata.etl.EtlJob;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;

import java.io.*;
import java.util.Properties;

/**
 * Created by thomas young on 5/22/17.
 */
public class HBaseMetadataEtl extends EtlJob {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataEtl.class);


    @Deprecated
    public HBaseMetadataEtl(Integer dbId, Long whExecId) {
        super(null, dbId, whExecId);
    }

    public HBaseMetadataEtl(int dbId, long whExecId, Properties prop) {
        super(null, dbId, whExecId, prop);
    }

    @Override
    public void extract()
            throws Exception {
        logger.info("Begin hbase metadata extract! - " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
      /*  HBaseMetaExtractor metaExtractor = new HBaseMetaExtractor(this.prop);
        metaExtractor.startToExtractHBaseData();*/
    }

    @Override
    public void transform()
            throws Exception {
        logger.info("Begin hbase metadata transform : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
        // call a python script to do the transformation
        InputStream inputStream = classLoader.getResourceAsStream("jython/HBaseTransform.py");
        interpreter.execfile(inputStream);
        inputStream.close();
    }

    @Override
    public void load()
            throws Exception {
        logger.info("Begin hbase metadata load : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
        // load into mysql
        InputStream inputStream = classLoader.getResourceAsStream("jython/HBaseLoad.py");
        interpreter.execfile(inputStream);
        inputStream.close();
        logger.info("hbase metadata load finished : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
    }


}
