package metadata.etl.lhotse.extractor;

import metadata.etl.lhotse.LzTaskExecRecord;
import wherehows.common.schemas.LineageRecord;

import java.util.List;

/**
 * Created by thomas young on 3/31/17.
 */
public interface BaseLineageExtractor {
    public List<LineageRecord> getLineageRecord(String logLocation, LzTaskExecRecord lzTaskExecRecord, int defaultDatabaseId);
}
