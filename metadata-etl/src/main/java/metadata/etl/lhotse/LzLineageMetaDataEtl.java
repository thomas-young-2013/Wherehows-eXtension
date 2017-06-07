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

import metadata.etl.EtlJob;
import wherehows.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by thomas young on 3/28/17.
 */
public class LzLineageMetaDataEtl extends EtlJob {
    public Integer timeFrame = null;
    public Long endTimeStamp = null;
    Connection conn;

    public LzLineageMetaDataEtl(int instanceId) {
        super(instanceId, null, 0L);
        // for default
        try {
            setUp();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Used by backend server
     * @param appId The application id for the target Lhotse server
     * @param whExecId
     * @param properties All the properties for this Lhotse instance lineage collecting.
     */
    public LzLineageMetaDataEtl(int appId, long whExecId, Properties properties) {
        super(appId, null, whExecId, properties);
        this.timeFrame = Integer.valueOf(this.prop.getProperty(Constant.LZ_LINEAGE_ETL_LOOKBACK_MINS_KEY,
                "90")); //default lookback 90 mins
        if (this.prop.contains(Constant.LZ_LINEAGE_ETL_END_TIMESTAMP_KEY))
            this.endTimeStamp = Long.valueOf(this.prop.getProperty(Constant.LZ_LINEAGE_ETL_END_TIMESTAMP_KEY));
        try {
            setUp();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setUp()
            throws SQLException {
        String wherehowsHost = this.prop.getProperty(Constant.WH_DB_URL_KEY);
        String wherehowsUserName = this.prop.getProperty(Constant.WH_DB_USERNAME_KEY);
        String wherehowsPassWord = this.prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
        conn =
                DriverManager.getConnection(wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);
    }

    @Override
    public void extract()
            throws Exception {
        logger.info("Lhotse lineage metadata extract for {} begin", this.prop.getProperty(Constant.APP_ID_KEY));
        String emptyStaggingTable =
                "DELETE FROM stg_job_execution_data_lineage WHERE app_id = " + this.prop.getProperty(Constant.APP_ID_KEY);
        conn.createStatement().execute(emptyStaggingTable);
        String emptyStableTable =
                "DELETE FROM job_execution_data_lineage WHERE app_id = " + this.prop.getProperty(Constant.APP_ID_KEY);
        conn.createStatement().execute(emptyStableTable);

        LzLineageExtractorMaster lzLineageExtractorMaster = new LzLineageExtractorMaster(prop);
        logger.info("time frame is: " + timeFrame);
        // get lineage
        if (timeFrame != null && endTimeStamp != null && endTimeStamp != 0) {
            lzLineageExtractorMaster.run(timeFrame, endTimeStamp);
        }

        else if (timeFrame != null) {
            lzLineageExtractorMaster.run(timeFrame);
        } else {
            lzLineageExtractorMaster.run(10);
        }
        logger.info("Lhotse lineage metadata extract for {} end", this.prop.getProperty(Constant.APP_ID_KEY));
    }

    @Override
    public void transform()
            throws Exception {
        // Nothing
        logger.info("Lhotse lineage metadata transform");
    }

    @Override
    public void load()
            throws Exception {
        logger.info("Lhotse lineage metadata load");
        // insert into the final table
        // TODO: need to be insert on duplicate update, so the running flows can be updated
        String insertIntoFinalTable = "INSERT IGNORE INTO job_execution_data_lineage\n"
                + "( app_id, flow_exec_id, job_exec_id, job_exec_uuid, job_name, job_start_unixtime, job_finished_unixtime,\n"
                + "db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,\n"
                + "layout_id, storage_type, source_target_type, srl_no, source_srl_no, operation,\n"
                + "record_count, insert_count, delete_count, update_count, flow_path, created_date, wh_etl_exec_id)"
                + "SELECT app_id, flow_exec_id, job_exec_id, job_exec_uuid, job_name, job_start_unixtime, job_finished_unixtime,\n"
                + "db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,\n"
                + "layout_id, storage_type, source_target_type, srl_no, source_srl_no, operation,\n"
                + "record_count, insert_count, delete_count, update_count, flow_path, UNIX_TIMESTAMP(NOW()), " + this.prop
                .getProperty(Constant.WH_EXEC_ID_KEY) + " FROM stg_job_execution_data_lineage where app_id = " + this.prop
                .getProperty(Constant.APP_ID_KEY);
        logger.info("Lhotse Lineage load cmd :\n" + insertIntoFinalTable);
        conn.createStatement().execute(insertIntoFinalTable);

        logger.info("Lhotse lineage metadata ETL completed");
    }

    @Override
    public void close()
            throws SQLException {
        this.conn.close();
    }
}
