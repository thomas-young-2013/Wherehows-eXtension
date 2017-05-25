package metadata.etl.dataset.hbase;

import metadata.etl.dataset.hdfs.HdfsMetadataEtl;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Created by lake on 17-5-25.
 */
public class HBaseMetadataEtlTest {
    HBaseMetadataEtl ds;

    @BeforeTest
    public void setUp()
            throws Exception {
        ds = new HBaseMetadataEtl(2, 0L);
    }

    @Test(groups = {"needConfig"})
    public void testRun()
            throws Exception {
        ds.run();
    }

    @Test(groups = {"needConfig"})
    public void testExtract()
            throws Exception {
        ds.extract();
        //TODO check it copy back the files
    }

    @Test(groups = {"needConfig"})
    public void testTransform()
            throws Exception {
        ds.transform();
        //TODO check it generate the final csv file
    }

    @Test(groups = {"needConfig"})
    public void testLoad()
            throws Exception {
        ds.load();
    }
}
