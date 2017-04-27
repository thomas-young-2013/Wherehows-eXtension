package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 4/27/17.
 */
public class LhotseFlowDagRecord extends AbstractRecord {
    Integer appId;
    String flowPath;
    Integer flowVersion;
    String sourceJobPath;
    String targetJobPath;
    Long whExecId;

    public LhotseFlowDagRecord(Integer appId, String flowPath, Integer flowVersion, String sourceJobPath,
                                String targetJobPath, Long whExecId) {
        this.appId = appId;
        this.flowPath = flowPath;
        this.flowVersion = flowVersion;
        this.sourceJobPath = sourceJobPath;
        this.targetJobPath = targetJobPath;
        this.whExecId = whExecId;
    }

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(flowPath);
        allFields.add(flowVersion);
        allFields.add(sourceJobPath);
        allFields.add(targetJobPath);
        allFields.add(whExecId);
        return allFields;
    }
}
