package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;

import static com.datastax.driver.core.PoolingOptions.DEFAULT_POOL_TIMEOUT_MILLIS;


/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/14
 */
public class CassandraHelper {
    private static Configuration config;
    private static Cluster cluster;
    private static Session session;

    public static void init(Configuration originalConfig) {
        cluster = CassandraHelper.buildCluster(originalConfig);//Cluster.builder().addContactPoint(contactPoint).build();
        session = cluster.newSession();
        config = originalConfig;
    }

    public static List<Configuration> split(int adviceNumber, Configuration taskConfig) {

        List<Configuration> configurations = new ArrayList<Configuration>();
        String sql = taskConfig.get(Key.QUERY_SQL, String.class);
        if (!sql.isEmpty()) {
            buildConfigFromSql(sql, taskConfig);
            Configuration confTmp = taskConfig.clone();
            configurations.addAll(CassandraReaderSplitUtil.splitSqlWithPrimaryKey(confTmp, adviceNumber));
            //configurations.add(confTmp);
        }

        return configurations;

    }

    private static void buildConfigFromSql(String sql, Configuration taskConfig) {
        Map<String, Object> tableInfoFromSql = CassandraHelper.splitSQL(sql.toLowerCase());
        taskConfig.set(Constants.TABLE, tableInfoFromSql.get(Constants.TABLE));
        taskConfig.set(Constants.KETSPACE, tableInfoFromSql.get(Constants.KETSPACE));
        taskConfig.set(Constants.WHERE, tableInfoFromSql.get(Constants.WHERE));
        taskConfig.set(Constants.COLUMN, tableInfoFromSql.getOrDefault(Constants.COLUMN, "*"));
    }

    public static void validateConfiguration(Configuration originalConfig) {
        Map<String, Object> connection = originalConfig.getMap(Key.CONNECTION);
        String contactPoint = (String) connection.getOrDefault(Key.CONNECTION_HOST, null);
        if (null == contactPoint) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONFIG_ERROR,
                    "您未配置读取 cassandra 的信息.请正确配置 contactPoint 属性. ");
        }
        String querySqls = originalConfig.get(Key.QUERY_SQL, String.class);
        if (null == querySqls || querySqls.isEmpty()) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONFIG_ERROR,
                    "您未配置读取 cassandra 的信息. 请正确配置 sql 属性. ");

        }


    }

    public static Map<String, DataType> getColumnMap(Cluster cluster, Configuration taskConfig) {
        String sql = taskConfig.getNecessaryValue(Constants.SQL, CommonErrorCode.CONFIG_ERROR);
        Map<String, Object> split = splitSQL(sql);
        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace((String) split.get(Constants.KETSPACE)).getTable((String) split.get(Constants.TABLE));

        List<ColumnMetadata> columns = tableMetadata.getColumns();//可能会很大很大
        HashMap columnMap = new HashMap(columns.size());
        Set<String> columnsFromSelect = (HashSet<String>) split.get(Constants.COLUMN);
        columns.stream().forEach(x -> {
            if (columnsFromSelect.isEmpty() || columnsFromSelect.contains(x.getName()))
                columnMap.put(x.getName(), x.getType());
        });
        return columnMap;
    }

    public static Map<String, Object> splitSQL(String sql) {
        Map<String, Object> sqlMap = new HashMap<String, Object>();
        int keySpaceStartIndex = sql.indexOf("from ");
        if (keySpaceStartIndex == -1) {
            throw new InvalidQueryException(" SQL语法错误，请检查 ");
        }
      /*  if (sql.contains("max(") || sql.contains("min(") || sql.contains("avg")) {
            throw new InvalidQueryException(" 不支持的操作，请检查 ");
        }*/
        String keySpaceAndAfter = sql.substring(keySpaceStartIndex + 5);
        String[] keySpaceAndTable = keySpaceAndAfter.split("\\.");
        if (keySpaceAndTable.length < 2) {
            throw new InvalidQueryException(" SQL语法错误，请检查 ");
        }
        String keySpace = keySpaceAndTable[0];
        String table = keySpaceAndTable[1].split(" ")[0];
        sqlMap.put(Constants.KETSPACE, keySpace.trim());
        sqlMap.put(Constants.TABLE, table.trim());

        String selectAndCollumns = sql.substring(0, keySpaceStartIndex - 1);
        String columns = selectAndCollumns.replace("select", "").trim();
        String[] wheres = sql.toLowerCase().split(Constants.WHERE);
        if (wheres.length == 2) {
            sqlMap.put(Constants.WHERE, wheres[1].trim().toLowerCase().replace("allow", "").replace("filtering", ""));
        }

        sqlMap.put(Constants.COLUMN, columns);

        return sqlMap;
    }

    public static Map<String, Object> buildColumnInfo(Row row, ColumnDefinitions.Definition definition) {
        DataType dataType = definition.getType();
        String columnName = definition.getName();
        Map<String, Object> columnInfo = new HashMap<>(3);

        Object obj = row.getObject(columnName);
        columnInfo.put("columnType", dataType.getName());
        columnInfo.put("columnName", columnName);
        columnInfo.put("value", ObjectUtils.clone(obj)); //不clone 会出现循环引用？
        return columnInfo;

    }

    public static Cluster buildCluster(Configuration taskConfig) {
        Map<String, Object> connection = taskConfig.getMap(Key.CONNECTION);
        if (connection == null) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONFIG_ERROR,
                    "您未配置读取 connect 的信息.请正确配置 connect 属性. ");
        }
        PoolingOptions poolingOptions = new PoolingOptions();
        // 表示和集群里的机器至少有2个连接 最多有4个连接
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Key.CONNECTION_LOCAL_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Key.CONNECTION_LOCAL_MAX, 1))
                .setCoreConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Key.CONNECTION_DISTANCE_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Key.CONNECTION_DISTANCE_MAX, 1))
                .setPoolTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_POOL_READ_TIMEOUT, DEFAULT_POOL_TIMEOUT_MILLIS));
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_SOCKET_CONNECT_TIMEOUT, SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS));
        socketOptions.setReadTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_SOCKET_READ_TIMEOUT, SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS));

        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        // withCredentials:cassandra用户名密码 如果cassandra.yaml里authenticator：AllowAllAuthenticator 可以不用配置
        Cluster cluster = Cluster.builder()
                .addContactPoints(((String) connection.get(Key.CONNECTION_HOST)).split(","))
                .withSocketOptions(socketOptions)
                .withPoolingOptions(poolingOptions)
                .withPort((Integer) connection.getOrDefault(Key.CONNECTION_PORT, 9042))
                .withCredentials((String) connection.getOrDefault(Key.CONNECTION_USERNAME, ""), (String) connection.getOrDefault(Key.CONNECTION_PASSWORD, ""))
                .withSocketOptions(socketOptions).build();
        return cluster;
    }

    public static ResultSet readTable(String sql) {
        return session.execute(sql);
    }

    public static void close() {
        if (cluster != null) {
            cluster.close();
        }
        if (session != null) {
            session.close();
        }
    }
}
