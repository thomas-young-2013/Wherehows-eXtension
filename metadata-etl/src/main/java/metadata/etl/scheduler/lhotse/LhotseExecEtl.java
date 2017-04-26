package metadata.etl.scheduler.lhotse;

import metadata.etl.EtlJob;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by thomas young on 4/26/17.
 */
public class LhotseExecEtl extends EtlJob {
    public LhotseExecEtl(int appId, long whExecId) {
        super(appId, null, whExecId);
    }

    public LhotseExecEtl(int appId, long whExecId, Properties properties) {
        super(appId, null, whExecId, properties);
    }

    @Override
    public void extract()
            throws Exception {
        logger.info("In LhotseExecEtl java launch extract jython scripts");
        InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanExtract.py");
        interpreter.execfile(inputStream);
        inputStream.close();
    }

    @Override
    public void transform()
            throws Exception {
        logger.info("In LhotseExecEtl java launch transform jython scripts");
        InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanTransform.py");
        interpreter.execfile(inputStream);
        inputStream.close();
    }

    @Override
    public void load()
            throws Exception {
        logger.info("In LhotseExecEtl java launch load jython scripts");
        InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanLoad.py");
        interpreter.execfile(inputStream);
        inputStream.close();
        logger.info("In LhotseExecEtl java load jython scripts finished");
    }
}
