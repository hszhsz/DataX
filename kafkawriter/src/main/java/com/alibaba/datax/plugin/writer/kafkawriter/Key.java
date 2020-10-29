package com.alibaba.datax.plugin.writer.kafkawriter;

/**
 * @author dalizu on 2018/11/8.
 * @version v1.0
 * @desc
 */
public class Key {

    // must have
    public static final String TOPIC = "topic";

    public static final String BOOTSTRAP_SERVERS="bootstrapServers";

    // not must , not default
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    public static final String INDEX_TABLE_NAME = "indexTableName";
    public static final String META = "meta";
    public static final String METRIC_SCOPE = "metricScope";
    public static final String METRIC_SCOPE_ID = "metricScopeId";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String COLUMN = "column";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String TIMESTAMP_FORMAT = "timestampFormat";
    public static final String UNIQUE_ID = "unique_id";

}
