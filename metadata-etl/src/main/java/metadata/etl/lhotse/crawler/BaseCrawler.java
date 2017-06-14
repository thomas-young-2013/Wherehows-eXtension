package metadata.etl.lhotse.crawler;

import metadata.etl.lhotse.LzExecMessage;

/**
 * Created by thomas young on 5/22/17.
 */
public interface BaseCrawler {
    public String getRemoteLog(LzExecMessage message) throws Exception;
}
