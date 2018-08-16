package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ClassName CassandraHelper
 * @Description CassandraWriter工具类
 * @Author heshaozhong
 * @Date 下午8:06 2018/8/13
 */
public class CassandraHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);
    List<ColumnMetadata> columnListFromTable = null;
    private Cluster cluster;
    private Session session;
    private Configuration config = null;
    private List<Object> column = null;

    private Map<String, DataType> columnTypeMap = null;
    private List<Object> primaryKey = null;
    private String insertSql = "";
    private Gson gson = new Gson();
    private String table = "";

    private Map<String, Object> keyspace = new HashMap<String, Object>();

    private Map<String, Object> connection = new HashMap<String, Object>();

    public CassandraHelper(Configuration originalConfig) {
        this.config = originalConfig;
        init();
        connect();

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

    }

    public static void prepare(Configuration originConfig) {
        List<String> preSqls = originConfig.getList(Constants.PRESQL, String.class);
        if (preSqls.isEmpty())
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
    }

    private static Cluster  buildCluster(Configuration originConfig) {
        Map<String, Object> connection = originConfig.getMap(Constants.CONNECTION);
        // 表示和集群里的机器至少有2个连接 最多有4个连接
        Cluster cluster = Cluster.builder()
                .addContactPoints((String) connection.get(Constants.CONNECTION_HOST))
                .withPort((Integer) connection.get(Constants.CONNECTION_PORT))
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

    private void init() {
        column = config.getList(Constants.COLUMN);
        table = config.getString(Constants.TABLE);
        keyspace = config.getMap(Constants.KEYSPACE);
        connection = config.getMap(Constants.CONNECTION);
        primaryKey = config.getList(Constants.PRIMARY_KEY);

    }

    public void connect() {
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
                .withCredentials((String) connection.get(Constants.CONNECTION_USERNAME), (String) connection.get(Constants.CONNECTION_PASSWORD))
                .withPoolingOptions(poolingOptions).build();

        // 建立连接
        session = cluster.connect();

        columnListFromTable = buildColumnList();
        columnTypeMap = buildColumnMap();
        insertSql = buildSql();

    }

    /**
     * 创建键空间
     */
    public void createKeyspace() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE if not exists ")
                .append((String) keyspace.get(Constants.KEYSPACE_NAME))
                .append(" WITH replication = {'class': '")
                .append((String) keyspace.get(Constants.KEYSPACE_CLASS))
                .append("', 'replication_factor': '")
                .append((Integer) keyspace.get(Constants.KEYSPACE_REPLICATION_FACTOR))
                .append("'}");

        session.execute(sb.toString());
    }

    /**
     * 创建表
     */
    public void createTable(Record record) {
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
    public void insert(Record record) {
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

    public void close() {
        if (cluster != null) {
            cluster.close();
        }
        if (session != null) {
            session.close();
        }
    }

    public List<ColumnMetadata> buildColumnList() {
        Cluster clusterTmp = buildCluster(this.config);
        TableMetadata tableMetadata = clusterTmp.getMetadata().getKeyspace((String) keyspace.get(Constants.KEYSPACE_NAME)).getTable(table);
        List<ColumnMetadata> columns = tableMetadata.getColumns();//可能会很大很大
        return columns;
    }

    public Map<String, DataType> buildColumnMap() {
        List<ColumnMetadata> columns = columnListFromTable;//可能会很大很大
        HashMap columnMap = new HashMap(columns.size());
        columns.stream().forEach(x -> {
            columnMap.put(x.getName(), x.getType());
        });
        return columnMap;
    }

    public String buildSql() {
        StringBuilder sb = new StringBuilder();

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
            if (i != (columns.size() - 1)) {
                sb.append("?");
            }
        }
        sb.append(") ");
        return sb.toString();
    }

    public void insertBatch(List<Record> recordList) {
        PreparedStatement statement = session.prepare(insertSql);
        BoundStatement boundStatement = new BoundStatement(statement);
        BatchStatement batchStmt = new BatchStatement();

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
                            buildValue(columnTypeMap.get((String) colObj), record, i, obj);
                        }catch (Exception e){

                            LOG.error("buildColumnValue fail ,record:"+record.toString()+""+e.getMessage()+"");
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
                    }catch (Exception e){
                        LOG.error("buildColumnValue fail ,record:"+record.toString()+""+e.getMessage()+"");
                        break;
                    }
                }
            }

            batchStmt.add(boundStatement.bind(obj));
        }


        session.execute(batchStmt);
    }

    private void buildValue(DataType colType, Record record, int i, Object[] obj) {

        Column col = record.getColumn(i);

        if (col == null || col.getRawData() == null) {
            return;
        }
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
                obj[i] = gsonParseObjectFromString(gson, col.asString(), InetAddress.class);
                break;
            case TIMEUUID:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), UUID.class);
                break;

            case CUSTOM:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), ByteBuffer.class);
                break;
            case COUNTER:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), Long.class);
                break;

            case UUID:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), UUID.class);
                break;

            case DURATION:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), Duration.class);
                break;

            case LIST:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), List.class);
                break;

            case MAP:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), Map.class);
                break;

            case SET:

                obj[i] = gsonParseObjectFromString(gson, col.asString(), Set.class);
                break;

            case UDT:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), UDTValue.class);
                break;

            case TUPLE:
                obj[i] = gsonParseObjectFromString(gson, col.asString(), TupleValue.class);
                break;

        }


    }

    public Object gsonParseObjectFromString(Gson gson, String s, Class classType) {
        try {
            return gson.fromJson(s, classType);
        } catch (Exception e) {
            LOG.error(e.getMessage() + ";content=" + s);
            return null;
        }
    }
}
