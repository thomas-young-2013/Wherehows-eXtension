package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 4/27/17.
 */
public class LhotseFlowOwnerRecord extends AbstractRecord {
    Integer appId;
    String flowPath;
    String ownerId;
    String permissions;
    String ownerType;
    Long whExecId;

    public LhotseFlowOwnerRecord(Integer appId, String flowPath, String ownerId, String permissions, String ownerType,
                                  Long whExecId) {
        this.appId = appId;
        this.flowPath = flowPath;
        this.ownerId = ownerId;
        this.permissions = permissions;
        this.ownerType = ownerType;
        this.whExecId = whExecId;
    }

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(flowPath);
        allFields.add(ownerId);
        allFields.add(permissions);
        allFields.add(ownerType);
        allFields.add(whExecId);
        return allFields;
    }
}
