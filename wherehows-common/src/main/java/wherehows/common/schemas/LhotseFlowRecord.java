package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 4/26/17.
 */
public class LhotseFlowRecord extends AbstractRecord {
    Integer appId;
    String flowName;
    String flowGroup;
    String flowPath;
    Integer flowLevel;
    Long sourceModifiedTime;
    Integer sourceVersion;
    Character isActive;
    Long whExecId;

    public LhotseFlowRecord(Integer appId, String flowName, String flowGroup, String flowPath, Integer flowLevel, Long sourceModifiedTime,
                             Integer sourceVersion, Character isActive, Long whExecId) {
        this.appId = appId;
        this.flowName = flowName;
        this.flowGroup = flowGroup;
        this.flowPath = flowPath;
        this.flowLevel = flowLevel;
        this.sourceModifiedTime = sourceModifiedTime;
        this.sourceVersion = sourceVersion;
        this.isActive = isActive;
        this.whExecId = whExecId;
    }

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(flowName);
        allFields.add(flowGroup);
        allFields.add(flowPath);
        allFields.add(flowLevel);
        allFields.add(sourceModifiedTime);
        allFields.add(sourceVersion);
        allFields.add(isActive);
        allFields.add(whExecId);
        return allFields;
    }
}
