package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 5/11/17.
 */
public class LhotseFlowExecRecord extends AbstractRecord {
    Integer appId;
    String flowName;
    String flowPath;
    Integer sourceVersion;
    Integer flowExecId;
    String flowExecStatus;
    Integer attemptId;
    String executedBy;
    Long startTime;
    Long endTime;
    Long whExecId;

    public LhotseFlowExecRecord(Integer appId, String flowName, String flowPath, Integer sourceVersion, Integer flowExecId,
                                 String flowExecStatus, Integer attemptId, String executedBy, Long startTime, Long endTime, Long whExecId) {
        this.appId = appId;
        this.flowName = flowName;
        this.flowPath = flowPath;
        this.sourceVersion = sourceVersion;
        this.flowExecId = flowExecId;
        this.flowExecStatus = flowExecStatus;
        this.attemptId = attemptId;
        this.executedBy = executedBy;
        this.startTime = startTime;
        this.endTime = endTime;
        this.whExecId = whExecId;
    }

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(flowName);
        allFields.add(flowPath);
        allFields.add(sourceVersion);
        allFields.add(flowExecId);
        allFields.add(flowExecStatus);
        allFields.add(attemptId);
        allFields.add(executedBy);
        allFields.add(startTime);
        allFields.add(endTime);
        allFields.add(whExecId);
        return allFields;
    }
}

