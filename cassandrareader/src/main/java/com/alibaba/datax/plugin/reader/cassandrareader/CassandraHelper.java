package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import java.util.*;

import static com.alibaba.datax.plugin.reader.cassandrareader.Constant.*;

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
                confTmp.set(SQL, sql);
                confTmp.remove(Key.QUERY_SQL);
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
    //弃用
    public static Map<DataType, String> getColumnMap(Cluster cluster, Configuration taskConfig) {
        String sql = taskConfig.getNecessaryValue(SQL, CommonErrorCode.CONFIG_ERROR);
        Map<String, Object> split = splitSQL(sql);
        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace((String) split.get(KETSPACE)).getTable((String) split.get(TABLE));

        List<ColumnMetadata> columns = tableMetadata.getColumns();//可能会很大很大
        HashMap columnMap = new HashMap(columns.size());
        Set<String> columnsFromSelect = (HashSet<String>) split.get(COLUMN);
        columns.stream().forEach(x -> {
            if (columnsFromSelect.isEmpty() || columnsFromSelect.contains(x.getName()))
                columnMap.put(x.getType(), x.getName());
        });
        return columnMap;
    }

    private static Map<String, Object> splitSQL(String sql) {
        Map<String, Object> sqlMap = new HashMap<String, Object>();
        int keySpaceStartIndex = sql.indexOf("from ");
        if (keySpaceStartIndex == -1) {
            throw new InvalidQueryException(" SQL语法错误，请检查 ");
        }
        if (sql.contains("max(") || sql.contains("min(") || sql.contains("avg")) {
            throw new InvalidQueryException(" 不支持的操作，请检查 ");
        }
        String keySpaceAndAfter = sql.substring(keySpaceStartIndex + 5);
        String[] keySpaceAndTable = keySpaceAndAfter.split(".");
        if (keySpaceAndTable.length < 2) {
            throw new InvalidQueryException(" SQL语法错误，请检查 ");
        }
        String keySpace = keySpaceAndTable[0];
        String table = keySpaceAndTable[1].split(" ")[0];
        sqlMap.put(KETSPACE, keySpace);
        sqlMap.put(TABLE, table);

        String selectAndCollumns = keySpaceAndAfter.substring(0, keySpaceStartIndex - 1);
        String[] columns = selectAndCollumns.replace("select", "").trim().split(",");

        Set<String> columnSet = new HashSet<String>(Arrays.asList(columns));
        if (columns.length == 1 && columns[0].trim().equals("*")) {

        } else {
            sqlMap.put(COLUMN, columnSet);
        }
        return sqlMap;
    }

    public static Map<String, Object> buildColumnInfo(Row row, ColumnDefinitions.Definition definition) {
        DataType dataType = definition.getType();
        String columnName = definition.getName();
        Map<String, Object> columnInfo = new HashMap<>(10);
        Object obj = null;

        Object t = row.getObject(columnName);
        if (null != t) {
            obj = t;
        }
        if(obj!=null){
            columnInfo.put("columnType",dataType);
            columnInfo.put("columnName",columnName);
            columnInfo.put("value",t);
        }
        return columnInfo;


    }
}
