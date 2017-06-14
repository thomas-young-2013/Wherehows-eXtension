/**
 * Copyright 2017 tencent. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package metadata.etl.lhotse;

import wherehows.common.schemas.AbstractRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 3/30/17.
 */
public class LzTaskExecRecord extends AbstractRecord {
    public Integer appId;
    public String taskId;
    public Integer taskType;
    public String taskName;
    public Integer taskStartTime;
    public Integer taskEndTime;
    public String projectName;
    public String workflowName;
    public Integer flowId;

    @Override
    public List<Object> fillAllFields() {
        List<Object> allFields = new ArrayList<>();
        allFields.add(appId);
        allFields.add(taskId);
        allFields.add(taskType);
        allFields.add(taskName);
        allFields.add(taskStartTime);
        allFields.add(taskEndTime);
        allFields.add(projectName);
        allFields.add(workflowName);
        allFields.add(flowId);
        return allFields;
    }

    public LzTaskExecRecord(Integer appId, String taskId, Integer taskType, String taskName,
                            Integer taskStartTime, Integer taskEndTime) {
        this.appId = appId;
        this.taskId = taskId;
        this.taskType = taskType;
        this.taskName = taskName;
        this.taskStartTime = taskStartTime;
        this.taskEndTime = taskEndTime;
    }

    @Override
    public String toString() {
        return "LzTaskExecRecord{" +
                "appId=" + appId +
                ", taskId='" + taskId + '\'' +
                ", taskType=" + taskType +
                ", taskName='" + taskName + '\'' +
                ", taskStartTime=" + taskStartTime +
                ", taskEndTime=" + taskEndTime +
                ", projectName='" + projectName + '\'' +
                ", workflowName='" + workflowName + '\'' +
                ", flowId=" + flowId +
                '}';
    }
}
