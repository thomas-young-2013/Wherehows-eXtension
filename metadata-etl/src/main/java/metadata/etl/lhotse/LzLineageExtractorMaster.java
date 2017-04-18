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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.routing.SmallestMailboxPool;
import akka.util.Timeout;
import metadata.etl.lineage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import wherehows.common.Constant;
import wherehows.common.PathAnalyzer;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.writers.DatabaseWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by thomas young on 3/30/17.
 */
public class LzLineageExtractorMaster {
    Properties prop;
    private static final Logger logger =  LoggerFactory.getLogger(LzLineageExtractorMaster.class);
    public LzLineageExtractorMaster(Properties prop)
            throws Exception {
        this.prop = prop;
    }

    /**
     * Default 10 minutes
     * @throws Exception
     */
    public void run()
            throws Exception {
        run(10);
    }

    public void run(int timeFrame)
            throws Exception {
        run(timeFrame, System.currentTimeMillis());
    }

    /**
     * Entry point.
     * All recent finished azkaban jobs' lineage. Will write to database stagging table
     * @param timeFrame in minutes
     * @param endTimeStamp in millisecond
     * @throws Exception
     */
    public void run(int timeFrame, long endTimeStamp) throws Exception {
        // get recent finished job
        LzJobChecker lzJobChecker = new LzJobChecker(prop);
        List<LzTaskExecRecord> taskExecList = lzJobChecker.getRecentFinishedJobFromFlow(timeFrame, endTimeStamp);
        lzJobChecker.close();

        logger.info("Total number of lhotse jobs : {}", taskExecList.size());

        ActorSystem actorSystem = ActorSystem.create("LhotseLineageExtractor");
        int numOfActor = Integer.valueOf(prop.getProperty(Constant.LZ_LINEAGE_ACTOR_NUM, "50"));
        ActorRef lineageExtractorActor = actorSystem
                .actorOf(new SmallestMailboxPool(numOfActor).props(Props.create(LzLineageExtractorActor.class)),
                        "lhotseLineageExtractorActor");

        String wherehowsUrl = prop.getProperty(Constant.WH_DB_URL_KEY);
        String wherehowsUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
        String wherehowsPassWord = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
        String connUrl = wherehowsUrl + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord;
        Connection conn = DriverManager.getConnection(connUrl);
        DatabaseWriter databaseWriter = new DatabaseWriter(connUrl, "stg_job_execution_data_lineage");

        int timeout = 30; // default 30 minutes for one job
        if (prop.containsKey(Constant.LZ_LINEAGE_ACTOR_TIMEOUT_KEY))
            timeout = Integer.valueOf(prop.getProperty(Constant.LZ_LINEAGE_ACTOR_TIMEOUT_KEY));
        List<Future<Object>> result = new ArrayList<>();
        for (LzTaskExecRecord lzRecord: taskExecList) {
            LzExecMessage lzExecMessage = new LzExecMessage(lzRecord, prop);
            lzExecMessage.connection = conn;
            lzExecMessage.databaseWriter = databaseWriter;
            Timeout t = new Timeout(timeout, TimeUnit.SECONDS);
            Future<Object> fut = Patterns.ask(lineageExtractorActor, lzExecMessage, t);
            result.add(fut);
        }

        // join all threads
        Future<Iterable<Object>> seq = Futures.sequence(result, actorSystem.dispatcher());
        try {
            Await.result(seq, Duration.create(timeout + " seconds"));
        } catch (TimeoutException exception) {
            exception.printStackTrace();
        }
        databaseWriter.close();
        logger.info("All job finished lineage collecting!");
    }
}
