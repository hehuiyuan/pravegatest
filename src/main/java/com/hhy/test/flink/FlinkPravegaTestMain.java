package com.hhy.test.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** @Author hehuiyuan @Date 2021/12/21 10:46 AM */
public class FlinkPravegaTestMain {
    public static void main(String[] args) {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tenv = TableEnvironment.create(settings);
        tenv.executeSql(
                "CREATE TABLE user_behavior (\n"
                        + "    user_id STRING,\n"
                        + "    item_id BIGINT,\n"
                        + "    category_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    log_ts TIMESTAMP(3),\n"
                        + "    ts as log_ts + INTERVAL '1' SECOND,\n"
                        + "    watermark for ts as ts\n"
                        + "    )\n"
                        + "WITH (\n"
                        + "    'connector' = 'pravega',\n"
                        + "    'controller-uri' = 'tcp://127.0.0.1:9090',\n"
                        + "    'scope' = 'examples',\n"
                        + "    'scan.execution.type' = 'streaming',\n"
                        + "    'scan.streams' = 'helloStream1',\n"
                        + "    'scan.reader-group.name' = 'helloStream1',\n"
                        + "    'format' = 'json'\n"
                        + "    )");

        tenv.executeSql(
                "CREATE TABLE user_behavior_print (\n"
                        + "    user_id STRING,\n"
                        + "    item_id BIGINT,\n"
                        + "    category_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    log_ts TIMESTAMP(3)\n"
                        + "    )\n"
                        + "WITH (\n"
                        + "    'connector' = 'print'\n"
                        + "    )");

        tenv.executeSql("insert into user_behavior_print select user_id,item_id,category_id,behavior,log_ts from user_behavior");

    }
}
