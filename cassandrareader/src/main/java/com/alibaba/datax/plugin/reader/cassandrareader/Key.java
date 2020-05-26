package com.alibaba.datax.plugin.reader.cassandrareader;

public final class Key {


    public final static String QUERY_SQL = "querySql";

    public final static String CONNECTION = "connection";

    public final static String CONNECTION_HOST = "host";

    public final static String CONNECTION_PORT = "port";

    public final static String CONNECTION_USERNAME = "username";

    public final static String CONNECTION_PASSWORD = "password";

    public final static String CONNECTION_LOCAL_MIN = "local_min";

    public final static String CONNECTION_LOCAL_MAX = "local_max";

    public final static String CONNECTION_DISTANCE_MIN = "distance_min";

    public final static String CONNECTION_DISTANCE_MAX = "distance_max";

    public final static String CONNECTION_SOCKET_CONNECT_TIMEOUT = "socket_connect_timeout";
    public final static int CONNECTION_SOCKET_CONNECT_TIMEOUT_MILLS = 120000;
    public final static String CONNECTION_SOCKET_READ_TIMEOUT = "socket_read_timeout";
    public final static int CONNECTION_SOCKET_READ_TIMEOUT_MILLS = 120000;
    public final static String CONNECTION_POOL_READ_TIMEOUT = "pool_connect_timeout";
    public final static int CONNECTION_POOL_READ_TIMEOUT_MILLS = 30000;


    public static final String MODE = "mode";

    public final static String SPLIT_PK = "splitPk";

    public final static String ALLOWFILTER = "allowFilter";

    public final static String ALLOW_FILTERING = "ALLOW FILTERING";

}
