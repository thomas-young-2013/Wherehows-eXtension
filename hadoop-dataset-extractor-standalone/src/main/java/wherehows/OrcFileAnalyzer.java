/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/18/15.
 */
public class OrcFileAnalyzer extends FileAnalyzer {

    private static Logger LOG = LoggerFactory.getLogger(OrcFileAnalyzer.class);

    public OrcFileAnalyzer(FileSystem fs) {
        super(fs);
        STORAGE_TYPE = "orc";
    }

    @Override
    public DatasetJsonRecord getSchema(Path targetFilePath)
            throws IOException {
        DatasetJsonRecord datasetJsonRecord = null;
        try {
            Reader orcReader = OrcFile.createReader(fs, targetFilePath);
            String codec = String.valueOf(orcReader.getCompression());
            String schemaString = orcReader.getObjectInspector().getTypeName();
            String storage = STORAGE_TYPE;
            String abstractPath = targetFilePath.toUri().getPath();
            FileStatus fstat = fs.getFileStatus(targetFilePath);
            datasetJsonRecord =
                    new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
                            fstat.getPermission().toString(), codec, storage, "");
        } catch (Exception e) {
            LOG.info(e.getMessage() + " in orcFileAnalyzer get schema ");
        }

        return datasetJsonRecord;
    }

    @Override
    public SampleDataRecord getSampleData(Path targetFilePath)
            throws IOException {
        SampleDataRecord sampleDataRecord = null;
        try {
            Reader orcReader = OrcFile.createReader(fs, targetFilePath);
            RecordReader recordReader = orcReader.rows();
            int count = 0;
            List<Object> list = new ArrayList<Object>();
            Object row = null;
            while (recordReader.hasNext() && count < 10) {
                count++;
                row = recordReader.next(row);
                list.add(row.toString().replaceAll("[\\n\\r\\p{C}]", ""));
            }
            sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);
        } catch (Exception e) {
            LOG.info(e.getMessage() + " while  orcfileanalyzer get sampledata");

        }
        return sampleDataRecord;
    }
}
