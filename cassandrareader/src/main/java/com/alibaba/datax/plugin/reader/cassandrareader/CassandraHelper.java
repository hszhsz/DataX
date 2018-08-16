package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import java.util.*;


/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/14
 */
public class CassandraHelper {

    public static List<Configuration> split(int adviceNumber, Configuration originConfig) {

        List<Configuration> configurations = new ArrayList<Configuration>();
        List<String> querySqls = originConfig.getList(Key.QUERY_SQL, String.class);
        if (!querySqls.isEmpty()) {
            for (String sql : querySqls) {
                Configuration confTmp = originConfig.clone();
                confTmp.set(Constants.SQL, sql);
                confTmp.remove(Key.QUERY_SQL);
                configurations.add(confTmp);
            }
        }

        //TODO  根据主键范围确定每个task startkey,endkey
        return configurations;

    }

    public static void validateConfiguration(Configuration originalConfig) {
        // do not splitPk
        String contactPoint = originalConfig.getString(Key.CONTACTPOINT, null);
        if (null == contactPoint) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONFIG_ERROR,
                    "您未配置读取 cassandra 的信息.请正确配置 contactPoint 属性. ");
        }
        List<String> querySqls = originalConfig.getList(Key.QUERY_SQL, String.class);
        if (null == querySqls || querySqls.isEmpty()) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONFIG_ERROR,
                    "您未配置读取 cassandra 的信息. 请正确配置 contactPoint 属性. ");

        }


    }

    public static Map<String , DataType> getColumnMap(Cluster cluster, Configuration taskConfig) {
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
        sqlMap.put(Constants.KETSPACE, keySpace);
        sqlMap.put(Constants.TABLE, table);

        String selectAndCollumns = keySpaceAndAfter.substring(0, keySpaceStartIndex - 1);
        String[] columns = selectAndCollumns.replace("select", "").trim().split(",");

        Set<String> columnSet = new HashSet<String>(Arrays.asList(columns));
        if (columns.length == 1 && columns[0].trim().equals("*")) {

        } else {
            sqlMap.put(Constants.COLUMN, columnSet);
        }
        return sqlMap;
    }

    public static Map<String, Object> buildColumnInfo(Row row, ColumnDefinitions.Definition definition) {
        DataType dataType = definition.getType();
        String columnName = definition.getName();
        Map<String, Object> columnInfo = new HashMap<>(10);

        Object obj = row.getObject(columnName);

        if (obj != null) {
            columnInfo.put("columnType", dataType);
            columnInfo.put("columnName", columnName);
            columnInfo.put("value", obj);
        }
        return columnInfo;

    }

    public static Cluster initCluster(Configuration taskConfig) {
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
                .setMaxConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Key.CONNECTION_DISTANCE_MAX, 1));

        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        // withCredentials:cassandra用户名密码 如果cassandra.yaml里authenticator：AllowAllAuthenticator 可以不用配置
        Cluster cluster = Cluster.builder()
                .addContactPoints((String) connection.get(Key.CONNECTION_HOST))
                .withPort((Integer) connection.getOrDefault(Key.CONNECTION_PORT, 9042))
                .withCredentials((String) connection.getOrDefault(Key.CONNECTION_USERNAME, ""), (String) connection.getOrDefault(Key.CONNECTION_PASSWORD, ""))
                .withPoolingOptions(poolingOptions).build();
        return cluster;
    }
}
