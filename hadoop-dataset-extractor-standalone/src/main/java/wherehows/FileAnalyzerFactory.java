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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.DatasetSchemaRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/18/15.
 */
public class FileAnalyzerFactory {
    private static Logger LOG = LoggerFactory.getLogger(FileAnalyzerFactory.class);
    private List<FileAnalyzer> allFileAnalyzer = new ArrayList<FileAnalyzer>();

    FileSystem fs;

    public FileAnalyzerFactory(FileSystem fs) {
        this.fs = fs;
        LOG.info("FileAnalyzerFactory init success !");
        allFileAnalyzer.add(new AvroFileAnalyzer(fs));
        allFileAnalyzer.add(new OrcFileAnalyzer(fs));
        allFileAnalyzer.add(new XMLFileAnalyzer(fs));
    }

    // iterate through all possibilities
    public SampleDataRecord getSampleData(Path path, String abstractPath) throws IOException {
        SampleDataRecord sampleData = null;

        for (FileAnalyzer analyzer : allFileAnalyzer) {
            sampleData = analyzer.getSampleData(path);
            if (sampleData != null) {
                sampleData.setAbstractPath(abstractPath);
                break;
            }
        }
        return sampleData;

    }

    public DatasetJsonRecord getSchema(Path path, String abstractPath)
            throws IOException {
        DatasetJsonRecord schema = null;

        for (FileAnalyzer analyzer : allFileAnalyzer) {
            schema = analyzer.getSchema(path);
            if (schema != null) {
                schema.setAbstractPath(abstractPath);
                break;
            }
        }
        return schema;
    }

}



