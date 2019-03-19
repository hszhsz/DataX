package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.PoolingOptions.DEFAULT_POOL_TIMEOUT_MILLIS;

public class CassandraHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);
    private static List<ColumnMetadata> columnListFromTable = null;
    private static Cluster cluster;
    private static Session session;
    private static Configuration config = null;
    private static List<Object> column = null;
    public static boolean needCreateTable = false;

    private static Map<String, DataType> columnTypeMap = null;
    private static List<Object> primaryKey = null;
    private static String insertSql = "";
    private static String table = "";
    private static PreparedStatement statement;
//    BoundStatement boundStatement;

    private static Map<String, Object> keyspace = new HashMap<String, Object>();

    private static Map<String, Object> connection = new HashMap<String, Object>();

    public CassandraHelper() {
    }

    /**
     * 校验参数
     *
     * @param originalConfig
     */
    public static void validateParameter(Configuration originalConfig) {

    }

    /**
     * 清空表
     */
    public static void truncateTable(Configuration originalConfig) {

        Session session = buildCluster(originalConfig).connect();
        try {
            session.execute("truncate " + originalConfig.get(Constants.KEYSPACE) + "." + originalConfig.get(Constants.TABLE));
        } catch (Exception e) {
            LOG.error("truncateTable error {}", e.getMessage());
        }
        session.close();
    }

    public static void prepare(Configuration originConfig) {
        List<String> preSqls = originConfig.getList(Constants.PRESQL, String.class);
        if (preSqls == null || preSqls.isEmpty())
            return;
        Cluster clusterTmp = buildCluster(originConfig);
        Session session = clusterTmp.newSession();
        for (String sql : preSqls) {
            try {
                session.execute(sql);
            } catch (Exception e) {
                throw new InvalidQueryException(" SQL执行出错，请检查 sql=" + sql + "\n" + e.getMessage());
            }
        }
        if (null != session) {
            session.close();
        }
        clusterTmp.close();
    }

    private static Cluster buildCluster(Configuration originConfig) {
        Map<String, Object> connection = originConfig.getMap(Constants.CONNECTION);
        // 表示和集群里的机器至少有2个连接 最多有4个连接

        PoolingOptions poolingOptions = new PoolingOptions();
        // 表示和集群里的机器至少有2个连接 最多有4个连接
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Constants.CONNECTION_LOCAL_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Constants.CONNECTION_LOCAL_MAX, 1))
                .setCoreConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Constants.CONNECTION_DISTANCE_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Constants.CONNECTION_DISTANCE_MAX, 1))
                .setPoolTimeoutMillis((Integer) connection.getOrDefault(Constants.CONNECTION_POOL_READ_TIMEOUT, DEFAULT_POOL_TIMEOUT_MILLIS));
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis((Integer) connection.getOrDefault(Constants.CONNECTION_SOCKET_CONNECT_TIMEOUT, SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS));
        socketOptions.setReadTimeoutMillis((Integer) connection.getOrDefault(Constants.CONNECTION_SOCKET_READ_TIMEOUT, SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS));

        Cluster cluster = Cluster.builder()
                .addContactPoints((String) connection.get(Constants.CONNECTION_HOST))
                .withPort((Integer) connection.get(Constants.CONNECTION_PORT))
                .withSocketOptions(socketOptions)
                .withPoolingOptions(poolingOptions)
                .withCredentials((String) connection.get(Constants.CONNECTION_USERNAME), (String) connection.get(Constants.CONNECTION_PASSWORD))
                .build();
        return cluster;
    }

    public static String DataTypeConverter(String dataType) {
        String result = "";
        if (dataType == "BAD" || dataType == "NULL" || dataType == "STRING") {
            result = "text";
        } else if (dataType == "INT") {
            result = "int";
        } else if (dataType == "LONG") {
            result = "bigint";
        } else if (dataType == "DOUBLE") {
            result = "double";
        } else if (dataType == "STRING") {
            result = "text";
        } else if (dataType == "BOOL") {
            result = "boolean";
        } else if (dataType == "DATE") {
            result = "timestamp";
        } else if (dataType == "BYTES") {
            result = "text";
        }
        return result;
    }


    /**
     * 创建键空间
     */
    public static void createKeyspace(Configuration originalConfig) {
        StringBuilder sb = new StringBuilder();
        Map<String, Object> keyspace = originalConfig.getMap(Constants.KEYSPACE);
        String c = "SimpleStrategy";
        if (keyspace.containsKey(Constants.KEYSPACE_CLASS)) {
            String cc = (String) keyspace.get(Constants.KEYSPACE_CLASS);
            if (cc != null && !cc.isEmpty()) {
                c = cc;
            }
        }

        Session session = buildCluster(originalConfig).connect();
        sb.append("CREATE KEYSPACE if not exists ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(" WITH replication = {'class': '")
                .append(c)
                .append("', 'replication_factor': '")
                .append((Integer) keyspace.getOrDefault(Constants.KEYSPACE_REPLICATION_FACTOR, 1))
                .append("'}");

        LOG.info("createKeyspace sql : {}", sb.toString());

        try {
            session.execute(sb.toString());
        } catch (Exception e) {
            LOG.error("create createKeyspace error {}", e.getMessage());
        }
        session.close();
    }

    public static void init(Configuration originalConfig) {
        config = originalConfig;
        table = config.getString(Constants.TABLE);
        keyspace = config.getMap(Constants.KEYSPACE);
        connection = config.getMap(Constants.CONNECTION);
        primaryKey = config.getList(Constants.PRIMARY_KEY);

        connect();
        initTableMeta();
    }

    public static void initTableMeta() {
        column = config.getList(Constants.COLUMN);
        columnListFromTable = buildColumnList();
        columnTypeMap = buildColumnMap();
        insertSql = buildSql();
        statement = session.prepare(insertSql);
        LOG.info("columnListFromTable {}", columnListFromTable);
        LOG.info("columnTypeMap {}", columnTypeMap);
        LOG.info("insertSql {}", insertSql);

    }

    public static void connect() {
        PoolingOptions poolingOptions = new PoolingOptions();
        // 表示和集群里的机器至少有2个连接 最多有4个连接
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Constants.CONNECTION_LOCAL_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.LOCAL, (Integer) connection.getOrDefault(Constants.CONNECTION_LOCAL_MAX, 100))
                .setCoreConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Constants.CONNECTION_DISTANCE_MIN, 1))
                .setMaxConnectionsPerHost(HostDistance.REMOTE, (Integer) connection.getOrDefault(Constants.CONNECTION_DISTANCE_MAX, 100));

        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        // withCredentials:cassandra用户名密码 如果cassandra.yaml里authenticator：AllowAllAuthenticator 可以不用配置
        cluster = Cluster.builder()
                .addContactPoints((String) connection.get(Constants.CONNECTION_HOST))
                .withPort((Integer) connection.get(Constants.CONNECTION_PORT))
                .withCredentials((String) connection.get(Constants.CONNECTION_USERNAME), (String) connection.get(Constants.CONNECTION_PASSWORD))
                .withPoolingOptions(poolingOptions).build();

        // 建立连接
        session = cluster.connect();

    }

    /**
     * 创建表
     */
    public static void createTable(Record record) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE if not exists ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(table)
                .append(" (");

        for (int i = 0; i < record.getColumnNumber(); i++) {
            sb.append(column.get(i))
                    .append(" ").append(DataTypeConverter(record.getColumn(i).getType().name()))
                    .append(",");
        }
        if (primaryKey.isEmpty()) {
            throw DataXException.asDataXException(CassandraWriterErrorCode.CREATE_CASSANDRA_ERROR, "primaryKey is cannot be empty");
        }
        sb.append("PRIMARY KEY (")
                .append(primaryKey.toString()
                        .replace("[", "")
                        .replace("]", "")
                        .replace("\"", ""))
                .append("))");
        LOG.info("createKeyspace sql : {}", sb.toString());

        session.execute(sb.toString());
    }

    /**
     * 插入
     */
    public static void insert(Record record) {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(table)
                .append(" (");

        for (int i = 0; i < column.size(); i++) {
            sb.append(column.get(i));
            if (i != (column.size() - 1)) {
                sb.append(",");
            }
        }

        sb.append(") VALUES ( ");

        for (int j = 0; j < record.getColumnNumber(); j++) {
            Column column = record.getColumn(j);
            switch (column.getType()) {
                case INT:
                    sb.append(column.asBigInteger());
                    break;
                case BOOL:
                    sb.append(column.asBoolean());
                    break;
                case DATE:
                    sb.append(column.asDate());
                    break;
                case LONG:
                    sb.append(column.asLong());
                    break;
                case BYTES:
                    sb.append("'").append(column.asBytes().toString()).append("'");
                    break;
                case DOUBLE:
                    sb.append(column.asDouble());
                    break;
                case STRING:
                    sb.append("'").append(column.asString()).append("'");
                    break;
                case NULL:
                case BAD:
                    break;
            }
            if (j != (record.getColumnNumber() - 1)) {
                sb.append(",");
            }
        }

        sb.append(" )");

        session.execute(sb.toString());
    }

    public static void close() {
        if (cluster != null) {
            cluster.close();
        }
        if (session != null) {
            session.close();
        }
    }

    public static List<ColumnMetadata> buildColumnList() {
        Cluster clusterTmp = buildCluster(config);
        KeyspaceMetadata key = clusterTmp.getMetadata().getKeyspace((String) keyspace.get(Constants.KEYSPACE_NAME));
        if (key == null) {
            return null;
        }

        TableMetadata tableMetadata = key.getTable(table);
        if (tableMetadata == null) {
            return null;
        }
        List<ColumnMetadata> columns = tableMetadata.getColumns();//可能会很大很大
        return columns;
    }

    public static Map<String, DataType> buildColumnMap() {
        if (columnListFromTable == null) {
            return null;
        }
        List<ColumnMetadata> columns = columnListFromTable;//可能会很大很大
        HashMap columnMap = new HashMap(columns.size());
        columns.stream().forEach(x -> {
            columnMap.put(x.getName(), x.getType());
        });
        return columnMap;
    }

    public static String buildSql() {
        StringBuilder sb = new StringBuilder();
        if (columnListFromTable == null || columnListFromTable.isEmpty()) {
            return null;
        }
        List<Object> columns = new ArrayList<>();
        if (!column.isEmpty()) {
            columns = column;
        } else {
            columns = columnListFromTable.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        }

        sb.append("INSERT INTO ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(table)
                .append(" (");

        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i));
            if (i != (columns.size() - 1)) {
                sb.append(",");
            }
        }

        sb.append(") VALUES ( ");
        for (int i = 0; i < columns.size(); i++) {
            sb.append("?");
            if (i != (columns.size() - 1)) {
                sb.append(",");
            }
        }
        sb.append(") ");
        int ttl = config.getInt(Constants.TTL, 0);
        if (ttl > 0) {
            sb.append("USING ttl ");
            sb.append(ttl);
        }
        return sb.toString();
    }

    public static void insert(String sql) {
        session.execute(sql);

    }

    public static void insertBatch(List<Record> recordList) {
        BatchStatement batchStmt = new BatchStatement();
        List<BoundStatement> boundStatementList = new ArrayList<>(recordList.size());
        for (Record record : recordList) {
            Object[] obj;
            int number = record.getColumnNumber();
            int tableColumnLength = columnListFromTable.size();
            int columnLength = column.size();
            if (columnLength > 0) {//指定了插入的列名
                obj = new Object[columnLength];
                for (int i = 0; i < columnLength; i++) {
                    Object colObj = column.get(i);
                    if (i >= number) {
                        obj[i] = null;
                    } else {
                        try {
                            buildValue(columnTypeMap.get(((String) colObj).trim().replace("\"", "")), record, i, obj);
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOG.error("buildColumnValue fail ,record:" + record.toString() + "" + e.getCause() + "");
                            break;
                        }
                    }
                }
            } else {//全部插入
                int colSize = Math.min(number, tableColumnLength);
                obj = new Object[colSize];
                for (int i = 0; i < colSize; i++) {
                    try {
                        buildValue(columnListFromTable.get(i).getType(), record, i, obj);
                    } catch (Exception e) {
                        LOG.error("buildColumnValue fail ,record:" + record.toString() + "" + e.getMessage() + "");
                        break;
                    }
                }
            }
            BoundStatement boundStatement = new BoundStatement(statement);
            boundStatementList.add(boundStatement.bind(obj));
        }
        batchStmt.addAll(boundStatementList);

        session.execute(batchStmt);
    }

    private static void buildValue(DataType colType, Record record, int i, Object[] obj) {

        Column col = record.getColumn(i);

        if (col == null || col.getRawData() == null || colType == null) {
            obj[i] = null;
            return;
        }
        //obj[i]=col.asBigInteger().intValue();

        switch (colType.getName()) {
            case INT:
                BigInteger bigInteger = col.asBigInteger();
                obj[i] = (bigInteger == null ? null : bigInteger.intValue());
                break;
            case SMALLINT:
                Short valueSMALLINT = new Short(col.asString());
                obj[i] = valueSMALLINT;
                break;
            case TINYINT:
                Byte valueTINYINT = new Byte(col.asString());
                obj[i] = valueTINYINT;
                break;
            case VARINT:
                BigInteger valueVARINT = col.asBigInteger();
                obj[i] = valueVARINT;
                break;
            case TIME:
                Long valueTIME = col.asLong();
                obj[i] = valueTIME;
                break;
            case ASCII:
                String valueASCII = col.asString();
                obj[i] = valueASCII;
                break;
            case BIGINT:
                Long valueBIGINT = col.asLong();
                obj[i] = valueBIGINT;
                break;
            case BLOB:
                ByteBuffer heapByteBuffer = ByteBuffer.allocate(col.asBytes().length);
                heapByteBuffer.put(col.asBytes());
                obj[i] = heapByteBuffer;
                break;
            case BOOLEAN:
                Boolean valueBOOLEAN = col.asBoolean();
                obj[i] = valueBOOLEAN;
                break;
            case DECIMAL:
                BigDecimal valueDECIMAL = col.asBigDecimal();
                obj[i] = valueDECIMAL;
                break;
            case DOUBLE:
                Double valueDOUBLE = col.asDouble();
                obj[i] = valueDOUBLE;
                break;
            case FLOAT:
                Float valueFLOAT = col.asDouble().floatValue();
                obj[i] = valueFLOAT;
                break;
            case VARCHAR:
                String valueVARCHAR = col.asString();
                obj[i] = valueVARCHAR;
                break;
            case TEXT:
                String valueTEXT = col.asString();
                obj[i] = valueTEXT;
                break;
            case TIMESTAMP:
                Date valueTIMESTAMP = col.asDate();
                obj[i] = valueTIMESTAMP;
                break;
            case DATE:
                Date d = col.asDate();
                LocalDate valueDATE = LocalDate.fromMillisSinceEpoch(d.getTime());
                obj[i] = valueDATE;
                break;
            case INET:
                obj[i] = gsonParseObjectFromString(col.asString(), InetAddress.class);
                break;
            case TIMEUUID:
                obj[i] = gsonParseObjectFromString(col.asString(), UUID.class);
                break;

            case CUSTOM:
                obj[i] = gsonParseObjectFromString(col.asString(), ByteBuffer.class);
                break;
            case COUNTER:
                obj[i] = gsonParseObjectFromString(col.asString(), Long.class);
                break;

            case UUID:
                obj[i] = gsonParseObjectFromString(col.asString(), UUID.class);
                break;

            case DURATION:
                obj[i] = gsonParseObjectFromString(col.asString(), Duration.class);
                break;

            case LIST:
                List objList = (List) gsonParseObjectFromString(col.asString(), List.class);
                obj[i] = objList;
                break;
            case MAP:
                Map objMap = (Map) gsonParseObjectFromString(col.asString(), Map.class);
                obj[i] = objMap;
                break;
            case SET:
                Set objListSet = (Set) gsonParseObjectFromString(col.asString(), Set.class);
                obj[i] = objListSet;
                break;
            case UDT:
                obj[i] = gsonParseObjectFromString(col.asString(), UDTValue.class);
                break;
            case TUPLE:
                obj[i] = gsonParseObjectFromString(col.asString(), TupleValue.class);
                break;
        }


    }

    public static Object gsonParseObjectFromString(String s, Class classType) {
        try {
            return JSON.parseObject(s, classType);
        } catch (Exception e) {
            LOG.error(e.getMessage() + ";content=" + s);
            return null;
        }
    }
}
