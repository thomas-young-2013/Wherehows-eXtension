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
    List<FileAnalyzer> allFileAnalyzer;

    public FileAnalyzerFactory(FileSystem fs) {
        allFileAnalyzer = new ArrayList<FileAnalyzer>();
        allFileAnalyzer.add(new AvroFileAnalyzer(fs));
        allFileAnalyzer.add(new HiveExportFileAnalyzer(fs));
        allFileAnalyzer.add(new XMLFileAnalyzer(fs));

        // allFileAnalyzer.add(new OrcFileAnalyzer(fs));
        // linkedin specific
        // allFileAnalyzer.add(new BinaryJsonFileAnalyzer(fs));
    }

    // iterate through all possibilities
    public SampleDataRecord getSampleData(Path path, String abstractPath) {
        SampleDataRecord sampleData = null;
        for (FileAnalyzer fileAnalyzer : allFileAnalyzer) {
            try {
                sampleData = fileAnalyzer.getSampleData(path);
                sampleData.setAbstractPath(abstractPath);
            } catch (Exception ignored) {
                ignored.printStackTrace();
                System.out.println("[get sample data] Debug: " + ignored);
            }
            if (sampleData != null) {
                break;
            }
        }
        return sampleData;
    }

    public DatasetJsonRecord getSchema(Path path, String abstractPath)
            throws IOException {
        DatasetJsonRecord schema = null;
        for (FileAnalyzer fileAnalyzer : allFileAnalyzer) {
            System.out.println("try file analyzer: " + fileAnalyzer.STORAGE_TYPE);
            try {
                schema = fileAnalyzer.getSchema(path);
                schema.setAbstractPath(abstractPath);
            } catch (Exception ignored) {
                ignored.printStackTrace();
                System.out.println("[get schema] Debug: " + ignored);
            }
            if (schema != null) {
                break;
            }
        }
        return schema;
    }

    /*private FileAnalyzer getRightFileAnalyzer(Path path) throws NullPointerException{
        FileAnalyzer analyzer = null;
        String rightPath = path.toUri().getPath();
        if(rightPath.endsWith(".xml" )&& !rightPath.startsWith(".xml")){
            analyzer = new XMLFileAnalyzer(fs);
        }
        if(rightPath.endsWith(".avro") && !rightPath.startsWith(".avro")){
            analyzer = new AvroFileAnalyzer(fs);
        }
        if(rightPath.endsWith(".orc") && !rightPath.startsWith(".orc")){
            analyzer = new OrcFileAnalyzer(fs);
        }

        return analyzer;
    }*/
}



