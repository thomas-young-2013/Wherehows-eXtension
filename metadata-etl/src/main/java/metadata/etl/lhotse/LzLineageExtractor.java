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

import metadata.etl.lhotse.crawler.LhotseExecLogCrawler;
import metadata.etl.lhotse.crawler.LhtoseConfCrawler;
import metadata.etl.lhotse.extractor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.LineageCombiner;
import wherehows.common.schemas.LineageRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thomas young on 3/28/17.
 */
public class LzLineageExtractor {

    private static final Logger logger = LoggerFactory.getLogger(LzLineageExtractor.class);
    /**
     * Reference: Get one job's lineage.
     * Process :
     * 1 get execution log from lhotse base server
     * 2 get hadoop job id from execution log
     * 3 get input, output from execution log and hadoop conf, normalize the path
     * 4 construct the Lineage Record
     *
     * @return one lhotse task's lineage
     */
    public static List<LineageRecord> extractLineage(LzExecMessage message)
            throws Exception {

        List<LineageRecord> jobLineage = new ArrayList<>();
        LzTaskExecRecord lzRecord = message.lzTaskExecRecord;
        String localLogLocation = new LhtoseConfCrawler().getRemoteLog(message);
        String logPath = null;

        BaseLineageExtractor lineageExtractor = null;
        switch (lzRecord.taskType) {
            case 72:
                lineageExtractor = new Hive2HdfsLineageExtractor();
                break;
            case 70:
                lineageExtractor = new HiveSqlLineageExtractor();
                break;
            case 75:
                lineageExtractor = new Hdfs2HiveLineageExtractor();
                break;
            case 92:
                logPath = new LhotseExecLogCrawler().getRemoteLog(message);
                lineageExtractor = new MRSubmitLineageExtractor();
                break;
            case 39:
                logPath = new LhotseExecLogCrawler().getRemoteLog(message);
                lineageExtractor = new SparkLineageExtractor();
                // lineageExtractor = new SparkSubmitLineageExtractor();
                break;
            default:
                throw new Exception("Not Supported Task Type!");
        }
        LineageCombiner lineageCombiner = new LineageCombiner(message.connection);
        Integer defaultDatabaseId = Integer.valueOf(message.prop.getProperty(Constant.LZ_DEFAULT_HADOOP_DATABASE_ID_KEY));
        if (lineageExtractor != null) {
            List<LineageRecord> lineageRecords = lineageExtractor.getLineageRecord(localLogLocation, message,
                    defaultDatabaseId, logPath);
            try {
                logger.info("start lineage combiner.");
                lineageCombiner.addAll(lineageRecords);
                logger.info("get combined lineage.");
                jobLineage.addAll(lineageCombiner.getCombinedLineage());
            } catch (Exception e) {
                e.printStackTrace();
                logger.info(e.getMessage());
                return lineageRecords;
            }
        }
        return jobLineage;
    }

    /**
     * Extract and write to database
     * @param message
     * @throws Exception
     */
    public static void extract(LzExecMessage message)
            throws Exception {
        try{
            List<LineageRecord> result = extractLineage(message);
            for (LineageRecord lr : result) {
                message.databaseWriter.append(lr);
            }
            logger.info(String.format("%03d lineage records extracted from [%s]", result.size(), message.toString()));
            message.databaseWriter.flush();
        } catch (Exception e) {
            logger.error(String.format("Failed to extract lineage info from [%s].\n%s", message.toString(), e.getMessage()));
        }
    }
}
