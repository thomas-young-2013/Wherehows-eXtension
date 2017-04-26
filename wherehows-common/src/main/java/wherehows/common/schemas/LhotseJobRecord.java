package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 4/26/17.
 */

public class LhotseJobRecord extends AbstractRecord {
    Integer appId;
    String flowPath;
    Integer sourceVersion;
    String jobName;
    String jobPath;
    String jobType;
    String refFlowPath;
    Character isCurrent;
    Long whExecId;

    public LhotseJobRecord(Integer appId, String flowPath, Integer sourceVersion, String jobName, String jobPath,
                            String jobType, Character isCurrent, Long whExecId) {
        this.appId = appId;
        this.flowPath = flowPath;
        this.sourceVersion = sourceVersion;
        this.jobName = jobName;
        this.jobPath = jobPath;
        this.jobType = jobType;
        this.isCurrent = isCurrent;
        this.whExecId = whExecId;
    }

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(flowPath);
        allFields.add(sourceVersion);
        allFields.add(jobName);
        allFields.add(jobPath);
        allFields.add(jobType);
        allFields.add(refFlowPath);
        allFields.add(isCurrent);
        allFields.add(whExecId);
        return allFields;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getFlowPath() {
        return flowPath;
    }

    public void setFlowPath(String flowPath) {
        this.flowPath = flowPath;
    }

    public Integer getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(Integer sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobPath() {
        return jobPath;
    }

    public void setJobPath(String jobPath) {
        this.jobPath = jobPath;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getRefFlowPath() {
        return refFlowPath;
    }

    public void setRefFlowPath(String refFlowPath) {
        this.refFlowPath = refFlowPath;
    }

    public Character getIsCurrent() {
        return isCurrent;
    }

    public void setIsCurrent(Character isCurrent) {
        this.isCurrent = isCurrent;
    }

    public Long getWhExecId() {
        return whExecId;
    }

    public void setWhExecId(Long whExecId) {
        this.whExecId = whExecId;
    }
}
