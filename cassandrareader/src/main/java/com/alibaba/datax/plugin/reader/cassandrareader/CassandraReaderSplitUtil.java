package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.datastax.driver.core.*;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.datax.common.exception.CommonErrorCode.RUNTIME_ERROR;
import static com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil.DATABASE_TYPE;

/**
 * Desc: 参考 ReaderSplitUtil
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/16
 */
public class CassandraReaderSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraReaderSplitUtil.class);

    public static List<Configuration> splitSqlWithPrimaryKey(Configuration taskConfig, int adviceNumber) {
        List<Configuration> configurationList = new ArrayList<>(adviceNumber);
        String splitPKey = taskConfig.getString(Key.SPLIT_PK);
        if (splitPKey == null || adviceNumber == 1) {//无需切割
            configurationList.add(taskConfig.clone());
            return configurationList;
        }
        if (!taskConfig.getBool(Key.ALLOWFILTER, false)) {
            LOG.info("配置不允许 where 切分");
            configurationList.add(taskConfig.clone());
            return configurationList;
        }
        TableMetadata tableMetadata = getTableMeta(taskConfig, taskConfig.getString(Constants.KETSPACE),
                taskConfig.getString(Constants.TABLE));
        Set primaryKeys = tableMetadata.getPrimaryKey().stream().map(x -> x.getName()).collect(Collectors.toSet());
        if (!primaryKeys.contains(splitPKey)) {// 考虑性能不使用 TODO test
            LOG.info("splitPk 需要为 primaryKey");
            configurationList.add(taskConfig.clone());
            return configurationList;
        }
        return doSplit(taskConfig, adviceNumber);


    }

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber) {


        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        Configuration sliceConfig = originalSliceConfig.clone();

        List<Configuration> splittedSlices = splitSingleTable(sliceConfig, adviceNumber);

        splittedConfigs.addAll(splittedSlices);
        return splittedConfigs;

    }


    public static List<Configuration> splitSingleTable(
            Configuration configuration, int adviceNum) {
        List<Configuration> pluginParams = new ArrayList<Configuration>();
        List<String> rangeList;
        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Constants.COLUMN);
        String table = configuration.getString(Constants.TABLE);
        String keySpace = configuration.getString(Constants.KETSPACE);

        String where = configuration.getString(Constants.WHERE, null);

        //String splitMode = configuration.getString(Key.SPLIT_MODE, "");
        //if (Constant.SPLIT_MODE_RANDOMSAMPLE.equals(splitMode) && DATABASE_TYPE == DataBaseType.Oracle) {

        Pair<Object, Object> minMaxPK = getPkRange(configuration);
        if (null == minMaxPK) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
        }

        //合并 a>1 , a>=2
        if (where != null && !where.trim().isEmpty()) {
            List<String> conditionList = new ArrayList<>();
            String[] conditions = where.trim().split("and");
            for (String condition : conditions) {
                String[] arrs = condition.split(">|<|>=|<=|=");//get colname
                if (arrs.length >= 2) {
                    if (!arrs[0].equals(splitPkName))
                        conditionList.add(condition);
                }
            }
            if (conditionList.isEmpty()) {
                where = "";
            } else {
                where = Joiner.on(" and ").join(conditionList);
            }
        }

        configuration.set(Key.QUERY_SQL, buildQuerySql(column, keySpace, table, where));

        if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
            // 切分后获取到的start/end 有 Null 的情况
            pluginParams.add(configuration);
            return pluginParams;
        }

        TableMetadata tableMetadata = getTableMeta(configuration, keySpace, table);
        DataType.Name pkType = tableMetadata.getColumn(splitPkName).getType().getName();

        if (isStringType(pkType)) {
            rangeList = CassandraRangeSplitWrap.splitAndWrap(
                    String.valueOf(minMaxPK.getLeft()),
                    String.valueOf(minMaxPK.getRight()), adviceNum,
                    splitPkName, "'", DATABASE_TYPE.MySql);
        } else if (isLongType(pkType)) {
            rangeList = CassandraRangeSplitWrap.splitAndWrap(
                    new BigInteger(minMaxPK.getLeft().toString()),
                    new BigInteger(minMaxPK.getRight().toString()),
                    adviceNum, splitPkName);
        } else {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
        }


        boolean hasWhere = StringUtils.isNotBlank(where);
        String tempQuerySql;
        List<String> allQuerySql = new ArrayList<String>();
        if (null != rangeList && !rangeList.isEmpty()) {
            for (String range : rangeList) {
                Configuration tempConfig = configuration.clone();
                tempQuerySql = buildQuerySql(column, keySpace, table, where)
                        + (hasWhere ? " and " : " where ") + range;
                tempQuerySql = tempQuerySql.concat(" allow filtering");
                allQuerySql.add(tempQuerySql);
                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        }
        // deal pk is null

        LOG.info("After split(), allQuerySql=[\n{}\n].",
                StringUtils.join(allQuerySql, "\n"));

        return pluginParams;
    }

    private static TableMetadata getTableMeta(Configuration configuration, String keySpace, String table) {
        Cluster cluster = CassandraHelper.buildCluster(configuration);
        TableMetadata metadata = cluster.getMetadata().getKeyspace(keySpace).getTable(table);
        cluster.close();
        return metadata;
    }

    public static String buildQuerySql(String column, String keySpace, String table,
                                       String where) {
        String querySql;

        if (StringUtils.isBlank(where)) {
            querySql = String.format("select %s from %s.%s",
                    column, keySpace, table);
        } else {
            querySql = String.format("select %s from %s.%s where %s", column, keySpace,
                    table, where);
        }

        return querySql;
    }

    @SuppressWarnings("resource")
    private static Pair<Object, Object> getPkRange(Configuration configuration) {
        String pkRangeSQL = genPKRangeSQL(configuration);

        Pair<Object, Object> minMaxPK = checkSplitPk(pkRangeSQL, configuration);
        return minMaxPK;
    }


    /**
     * 检测splitPk的配置是否正确。
     * configuration为null, 是precheck的逻辑，不需要回写PK_TYPE到configuration中
     */
    private static Pair<Object, Object> checkSplitPk(String pkRangeSQL, Configuration configuration) {
        ResultSet rs = null;
        ImmutablePair minMaxPK = null;
        String executeSql = pkRangeSQL;
        LOG.info("checkSplitPk:execute sql:" + pkRangeSQL);

        if (pkRangeSQL.toLowerCase().contains("where")) {
            executeSql = executeSql.concat("  allow filtering");
        }
        Cluster cluster = null;
        Session session = null;
        try {
            cluster = CassandraHelper.buildCluster(configuration);
            session = cluster.newSession();
            rs = session.execute(executeSql);
            Row row = rs.one();
            LOG.info("checkSplitPk:execute sql result:" + row.toString());

            if (!isPKTypeValid(rs.getColumnDefinitions())) {
                throw DataXException.asDataXException(CassandraReadErrorCode.CASSANDRA_SPLIT_PK_ERROR,
                        "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");

            }

            Object leftObject = row.getObject(0);
            Object rightObject = row.getObject(1);
            minMaxPK = new ImmutablePair(leftObject, rightObject);

        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(RUNTIME_ERROR, "DataX尝试切分表发生错误. 请检查您的配置并作出修改.", e);
        } finally {
            if (session != null) {
                session.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        }


        return minMaxPK;
    }

    private static boolean isPKTypeValid(ColumnDefinitions rsMetaData) {
        boolean ret = false;
        try {
            DataType.Name minType = rsMetaData.getType(0).getName();
            DataType.Name maxType = rsMetaData.getType(1).getName();

            boolean isNumberType = isLongType(minType);

            boolean isStringType = isStringType(minType);

            if (minType == maxType && (isNumberType || isStringType)) {
                ret = true;
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }

    // warn: Types.NUMERIC is used for oracle! because oracle use NUMBER to
    // store INT, SMALLINT, INTEGER etc, and only oracle need to concern
    // Types.NUMERIC
    private static boolean isLongType(DataType.Name type) {
        boolean isValidLongType = type == DataType.Name.BIGINT || type == DataType.Name.INT
                || type == DataType.Name.SMALLINT || type == DataType.Name.TINYINT || type == DataType.Name.VARINT
                || type == DataType.Name.TIME || type == DataType.Name.COUNTER;
        return isValidLongType;
    }

    private static boolean isStringType(DataType.Name type) {
        return type == DataType.Name.VARCHAR || type == DataType.Name.TEXT
                || type == DataType.Name.ASCII;
    }

    private static String genPKRangeSQL(Configuration configuration) {

        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String keySpace = configuration.getString(Constants.KETSPACE).trim();
        String table = configuration.getString(Constants.TABLE).trim();
        String where = configuration.getString(Constants.WHERE, null);
        return genPKSql(splitPK, keySpace, table, where);
    }

    public static String genPKSql(String splitPK, String keySpace, String table, String where) {

        String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s.%s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
                keySpace, table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE %s",
                    pkRangeSQL, where);
        }
        return pkRangeSQL;
    }


}


