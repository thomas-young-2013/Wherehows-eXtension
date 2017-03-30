package metadata.etl.lhotse;

import wherehows.common.schemas.AbstractRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 3/30/17.
 */
public class LzTaskExecRecord extends AbstractRecord {
    public Integer appId;
    public String taskId;
    public Integer taskType;
    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(taskId);
        allFields.add(taskType);
        return allFields;
    }

    public LzTaskExecRecord(Integer appId, String taskId, Integer taskType) {
        this.appId = appId;
        this.taskId = taskId;
        this.taskType = taskType;
    }
}
