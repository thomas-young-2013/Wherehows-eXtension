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

import wherehows.common.writers.DatabaseWriter;
import java.sql.Connection;
import java.util.Properties;

/**
 * Created by hadoop on 3/30/17.
 */
public class LzExecMessage {
    public LzTaskExecRecord lzTaskExecRecord;
    public Properties prop;


    public DatabaseWriter databaseWriter;
    public Connection connection;

    public LzExecMessage(LzTaskExecRecord lzTaskExecRecord, Properties prop) {
        this.lzTaskExecRecord = lzTaskExecRecord;
        this.prop = prop;
    }
}
