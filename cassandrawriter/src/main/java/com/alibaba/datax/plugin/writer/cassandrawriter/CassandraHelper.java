package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName CassandraHelper
 * @Description CassandraWriter工具类
 * @Author heshaozhong
 * @Date 下午8:06 2018/8/13
 */
public class CassandraHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);


    private Cluster cluster;

    private Session session;

    private Configuration config = null;


    private List<Object> column = null;
    private List<Object> primaryKey = null;

    private String table = "";

    private Map<String, Object> keyspace = new HashMap<String, Object>();

    private Map<String, Object> connection = new HashMap<String, Object>();

    public CassandraHelper(Configuration originalConfig) {
        this.config = originalConfig;
        column = originalConfig.getList(Constants.COLUMN);
        table = originalConfig.getString(Constants.TABLE);
        keyspace = originalConfig.getMap(Constants.KEYSPACE);
        connection = originalConfig.getMap(Constants.CONNECTION);
        primaryKey = originalConfig.getList(Constants.PRIMARY_KEY);
    }

    /**
     * 校验参数
     * @param originalConfig
     */
    public static void validateParameter(Configuration originalConfig) {

    }

    /**
     * 清空表
     */
    public static void truncateTable(Configuration originalConfig) {

    }

    public void connect()
    {
        PoolingOptions poolingOptions = new PoolingOptions();
        // 表示和集群里的机器至少有2个连接 最多有4个连接
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.get(Constants.CONNECTION_LOCAL_MIN))
                .setMaxConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.get(Constants.CONNECTION_LOCAL_MAX))
                .setCoreConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.get(Constants.CONNECTION_DISTANCE_MIN))
                .setMaxConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.get(Constants.CONNECTION_DISTANCE_MAX));

        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        // withCredentials:cassandra用户名密码 如果cassandra.yaml里authenticator：AllowAllAuthenticator 可以不用配置
        cluster = Cluster.builder()
                .addContactPoints((String) connection.get(Constants.CONNECTION_HOST))
                .withPort((Integer) connection.get(Constants.CONNECTION_PORT))
                .withCredentials((String)connection.get(Constants.CONNECTION_USERNAME), (String)connection.get(Constants.CONNECTION_PASSWORD))
                .withPoolingOptions(poolingOptions).build();

        // 建立连接
        session = cluster.connect();

    }

    /**
     * 创建键空间
     */
    public void createKeyspace()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE if not exists ")
                .append((String)keyspace.get(Constants.KEYSPACE_NAME))
                .append( " WITH replication = {'class': '")
                .append((String)keyspace.get(Constants.KEYSPACE_CLASS))
                .append("', 'replication_factor': '")
                .append((Integer) keyspace.get(Constants.KEYSPACE_REPLICATION_FACTOR))
                .append("'}");

        session.execute(sb.toString());
    }

    /**
     * 创建表
     */
    public void createTable(Record record)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE if not exists ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(table)
                .append(" (");

        for (int i=0; i< record.getColumnNumber(); i++) {
           sb.append(column.get(i))
                    .append(" ").append(DataTypeConverter(record.getColumn(i).getType().name()))
                    .append(",");
        }
        sb.append("PRIMARY KEY (")
                .append(primaryKey.toString()
                        .replace("[", "")
                        .replace("]", "")
                        .replace("\"", ""))
                .append("))");

        session.execute(sb.toString());
    }

    /**
     * 插入
     */
    public void insert(Record record)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(table)
                .append(" (");

        for(int i = 0; i < column.size(); i++ ) {
            sb.append(column.get(i));
            if(i != (column.size() -1)) {
                sb.append(",");
            }
        }

        sb.append(") VALUES ( ");

        for(int j = 0; j< record.getColumnNumber(); j++ ) {
            Column column = record.getColumn(j);
            switch (column.getType()) {
                case INT: sb.append(column.asBigInteger()); break;
                case BOOL: sb.append(column.asBoolean()); break;
                case DATE: sb.append(column.asDate()); break;
                case LONG: sb.append(column.asLong()); break;
                case BYTES: sb.append("'").append(column.asBytes().toString()).append("'"); break;
                case DOUBLE: sb.append(column.asDouble()); break;
                case STRING: sb.append("'").append(column.asString()).append("'"); break;
                case NULL:
                case BAD: break;
            }
            if(j != (record.getColumnNumber() -1)) {
                sb.append(",");
            }
        }

        sb.append(" )");

        session.execute(sb.toString());
    }

    public void close() {
        cluster.close();
        session.close();
    }

    public static String DataTypeConverter(String dataType) {
        String result = "";
        if(dataType == "BAD" || dataType=="NULL" || dataType == "STRING") {
            result = "text";
        }
        else if(dataType == "INT") {
            result = "int";
        }
        else if(dataType == "LONG") {
            result = "bigint";
        }
        else if (dataType == "DOUBLE" ) {
            result = "double";
        }
        else if (dataType == "STRING") {
            result = "text";
        }
        else if(dataType == "BOOL") {
            result = "boolean";
        }
        else if(dataType == "DATE") {
            result = "timestamp";
        }
        else if(dataType == "BYTES") {
            result = "text";
        }
        return result;
    }
}
