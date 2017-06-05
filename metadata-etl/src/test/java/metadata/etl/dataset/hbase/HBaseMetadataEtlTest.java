package metadata.etl.dataset.hbase;



/**
 * Created by lake on 17-5-25.
 */
public class HBaseMetadataEtlTest {


   public static void main (String []args) throws Exception{
       HBaseMetadataEtl ds = new HBaseMetadataEtl(0,1L);

       ds.extract();

   }
}
