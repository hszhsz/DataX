package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Map<String, Object> tableInfoFromSql = CassandraHelper.parseSQL(sql.toLowerCase());
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
        Map<String, Object> split = parseSQL(sql);
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
        int keySpaceStartIndex = sql.toLowerCase().indexOf("from ");
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


    public static Map<String, Object> parseSQL(String sql) {
        Map<String, Object> sqlMap = new HashMap<String, Object>();
        String parseSql;
        if (StringUtils.endsWithIgnoreCase(sql.trim(), Key.ALLOW_FILTERING)){
            parseSql = StringUtils.removeEndIgnoreCase(sql.trim(), Key.ALLOW_FILTERING);
        } else {
            parseSql = sql;
        }
        try {
            Statement stmt = CCJSqlParserUtil.parse(parseSql);
            Select selectStatement = (Select) stmt;
            PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
            List<SelectItem> selectItems = plainSelect.getSelectItems();
            List<String> colsNames = Lists.newArrayList();
            for (SelectItem selectItem : selectItems) {
                selectItem.accept(new SelectItemVisitorAdapter() {
                    @Override
                    public void visit(SelectExpressionItem item) {
                        colsNames.add(item.getExpression().getASTNode().jjtGetFirstToken().toString());
                    }
                });
            }
            Table table = (Table)plainSelect.getFromItem();
            String keyspaceName = table.getSchemaName();
            String tableName = table.getName();
            Expression where = plainSelect.getWhere();
            if (Strings.isNullOrEmpty(keyspaceName)) {
                throw new InvalidQueryException(" SQL语法错误，请检查 ");
            }

            sqlMap.put(Constants.KETSPACE, keyspaceName);
            sqlMap.put(Constants.TABLE, tableName);
            sqlMap.put(Constants.COLUMN, Joiner.on(",").join(colsNames));

            if(where != null){
                sqlMap.put(Constants.WHERE, where.toString());
            }

        } catch (Exception e) {
            throw new InvalidQueryException(e.getMessage());
        }

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
                .setPoolTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_POOL_READ_TIMEOUT, Key.CONNECTION_POOL_READ_TIMEOUT_MILLS));
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_SOCKET_CONNECT_TIMEOUT, Key.CONNECTION_SOCKET_CONNECT_TIMEOUT_MILLS));
        socketOptions.setReadTimeoutMillis((Integer) connection.getOrDefault(Key.CONNECTION_SOCKET_READ_TIMEOUT, Key.CONNECTION_SOCKET_READ_TIMEOUT_MILLS));

        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        // withCredentials:cassandra用户名密码 如果cassandra.yaml里authenticator：AllowAllAuthenticator 可以不用配置

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.ONE);
        Cluster cluster = Cluster.builder()
                .addContactPoints(((String) connection.get(Key.CONNECTION_HOST)).split(","))
                .withSocketOptions(socketOptions)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(queryOptions)
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


    public static void main(String[] args) {
        String sql = "SELECT case_uuid as a, org_uuid as b, dept_uuid , get_dept_uuid , org_abbr , org_shortname , case_name , case_code , case_year , case_date , prov_code , city_code , county_code , case_place , gis_x , gis_y , cause_of_action , case_property , is_no_master , party , co_party , register_time , reg_doc_no , case_source , case_brief , legal_argument , punish_argument , true_casevalue , false_casevalue , total_value , true_qty , false_qty , total_qty , forfeit , confiscate_amt , add_forfeit , total_amt , undertaker , handle_person , report_status , report_date , handle_proc , handle_result , " +
                "handle_status , is_finshed , finsh_date , is_filed , file_uuid , file_no , filed_date , is_major_case , priority_level , is_internet_case , is_network_case , goods_ident_status , goods_ident_date , goods_handle_status , goods_handle_date , get_node , is_trans , " +
                "receive_org_type , trans_date , is_hear , is_supv , is_den , is_overdue , overdue_days , is_recon , input_time ," +
                " is_process_complete , reg_status , is_criminal , back_date , goods_value , realize_amt , buy_amt , trans_receipt_date , " +
                "is_true_cig_case , uni_doc_no , upd_time , upd_person , evid_type_code , evid_type , is_market , market_name ," +
                "case_from , case_to , is_transport , transport_addr , transport_style , lst_modi_time , sync_status , remote_org_code , " +
                "sync_time , case_from_prov , case_from_city , case_from_county , case_to_prov , case_to_city , case_to_county  " +
                "FROM cdp.ods_zm_t_c_caseinfo ";
//        String sql = "select case_uuid from rt_basic where acc_pedal_stroke= 0 limit 5 ALLOW FILTERING";
        //splitSQL(sql);

        String sql1 = "SELECT check_date , org_code , index_id , check_table , index_value , data_source , begin_date , end_date  FROM cdp.dwd_zm_check_index_value  WHERE  check_date='20200116' ALLOW FILTERING";

        String sql2 = "SELECT mbr_id , receiver_name , receiver_mobile , detail_address , is_default , pt  FROM cdp.ads_cust_mbr_addr_detail  WHERE  pt='2020-06-03'";
        System.out.println(parseSQL(sql2));
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql2);
            Select selectStatement = (Select) stmt;
            PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
            List<SelectItem> selectItems = plainSelect.getSelectItems();
            List<String> colsNames = Lists.newArrayList();
            for (SelectItem selectItem : selectItems) {
                selectItem.accept(new SelectItemVisitorAdapter() {
                    @Override
                    public void visit(SelectExpressionItem item) {
                        System.out.println(item.getExpression().getASTNode().jjtGetFirstToken().toString());
                        colsNames.add(item.getExpression().getASTNode().jjtGetFirstToken().toString());
                    }
                });
            }
            Table table = (Table)plainSelect.getFromItem();
            System.out.println(plainSelect.getWhere().toString());
            System.out.println(table.getSchemaName());
            System.out.println(table.getName());
            System.out.println(colsNames);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
