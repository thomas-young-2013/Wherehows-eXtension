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
