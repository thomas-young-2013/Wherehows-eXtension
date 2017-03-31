package metadata.etl.lhotse.extractor;

import wherehows.common.schemas.LineageRecord;

import java.util.List;

/**
 * Created by hadoop on 3/31/17.
 */
public interface BaseLineageExtractor {
    public List<LineageRecord> getLineageRecord(String logLocation);
}
