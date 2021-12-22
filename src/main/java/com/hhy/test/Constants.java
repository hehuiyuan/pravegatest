package com.hhy.test;

/** @Author hehuiyuan @Date 2021/12/20 5:39 PM */
public class Constants {
    public static final String DEFAULT_SCOPE = "examples";
    public static final String DEFAULT_STREAM_NAME = "helloStream1";
    public static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";

    public static final String DEFAULT_ROUTING_KEY = null;
    public static final String DEFAULT_MESSAGE =
            "{\"user_id\":\"111\",\"item_id\":111111,\"category_id\":2222,\"behavior\":\"upsert\",\"log_ts\":\"2021-12-12 12:00:00\"}";
}
