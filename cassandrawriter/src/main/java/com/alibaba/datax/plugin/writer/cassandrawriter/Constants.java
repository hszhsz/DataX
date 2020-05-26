package com.alibaba.datax.plugin.writer.cassandrawriter;

/**
 * @ClassName Constants
 * @Description TODO
 * @Author heshaozhong
 * @Date 下午8:11 2018/8/13
 */
public final class Constants {
    public final static String CONNECTION = "connection";

    public final static String CONNECTION_HOST = "host";

    public final static String CONNECTION_PORT = "port";

    public final static String CONNECTION_USERNAME = "username";

    public final static String CONNECTION_PASSWORD = "password";

    public final static String CONNECTION_LOCAL_MIN = "local_min";

    public final static String CONNECTION_LOCAL_MAX = "local_max";

    public final static String CONNECTION_DISTANCE_MIN = "distance_min";

    public final static String CONNECTION_DISTANCE_MAX = "distance_max";
    public final static String CONNECTION_POOL_READ_TIMEOUT = "pool_connect_timeout";
    public final static int CONNECTION_POOL_READ_TIMEOUT_MILLS = 30000;

    public final static String CONNECTION_SOCKET_CONNECT_TIMEOUT = "socket_connect_timeout";
    public final static int CONNECTION_SOCKET_CONNECT_TIMEOUT_MILLS = 120000;
    public final static String CONNECTION_SOCKET_READ_TIMEOUT = "socket_read_timeout";
    public final static int CONNECTION_SOCKET_READ_TIMEOUT_MILLS = 120000;


    public final static String COLUMN = "column";
    public final static String PRIMARY_KEY = "primary_key";

    public final static String TABLE = "table";

    public final static String KEYSPACE = "keyspace";

    public final static String KEYSPACE_NAME = "name";

    public final static String KEYSPACE_CLASS = "class";

    public final static String KEYSPACE_REPLICATION_FACTOR = "replication_factor";

    public final static String TRUNCATE = "truncate";

    public static final String PRESQL = "preSql";
    //批量写入的大小
    public static final String BATCH_SIZE = "batchSize";
    //批量写入间隔时间
    public static final String DURATION = "duration";

    public static final String TTL = "ttl";

    public static final String CREATETABLE = "createTable";

    public static final String CREATEKETSPACE = "createKeySpace";


}
