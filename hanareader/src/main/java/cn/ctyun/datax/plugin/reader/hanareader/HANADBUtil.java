package cn.ctyun.datax.plugin.reader.hanareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * HANA DB UTIL
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public class HANADBUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HANADBUtil.class);
    /**
     * 获取HANA客户端连接
     * @param conf 配置文件类
     * @return HANA客户端连接
     */
    public static Connection connect(Configuration conf) {
        try {
            List<Object> connList = conf.getList(KeyConstant.CONN_MARK, Object.class);
            Configuration connConf = Configuration.from(connList.get(0).toString());

            String userName = conf.getString(KeyConstant.HANA_USERNAME);
            String password = conf.getString(KeyConstant.HANA_PASSWORD);

            String url = connConf.getString(KeyConstant.JDBC_URL);
            Class.forName(KeyConstant.DRIVER);
            return DriverManager.getConnection(url, userName, password);
        } catch (ClassNotFoundException e) {
            throw DataXException.asDataXException(HANAReaderErrorCode.CLASS_NOT_FOUND, "请联系开发人员");
        } catch (SQLException e) {
            throw DataXException.asDataXException(HANAReaderErrorCode.CONN_DB_ERROR, String.format("HANA连接失败[%s]", e.getMessage()));
        }
    }

    public static ResultSet query(Connection conn, String sql, int fetchSize) throws SQLException {
        return query(conn, sql, fetchSize, 172800);
    }

    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout) throws SQLException {
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    public static ResultSet query(Statement stmt, String sql) throws SQLException {
        LOG.info(sql);
        return stmt.executeQuery(sql);
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) { }
        }
        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) { }
        }
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) { }
        }
    }


    public static Record transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
        Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
        recordSender.sendToWriter(record);
        return record;
    }

    private static Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
        Record record = recordSender.createRecord();
        try {
            for(int i = 1; i <= columnNumber; ++i) {
                switch(metaData.getColumnType(i)) {
                    case -16:
                    case -15:
                    case -9:
                    case -1:
                    case 1:
                    case 12:
                        String rawData;
                        if (StringUtils.isBlank(mandatoryEncoding)) {
                            rawData = rs.getString(i);
                        } else {
                            rawData = new String(rs.getBytes(i) == null ? new byte[0] : rs.getBytes(i), mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;
                    case -7:
                    case 16:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;
                    case -6:
                    case -5:
                    case 4:
                    case 5:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;
                    case -4:
                    case -3:
                    case -2:
                    case 2004:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;
                    case 0:
                        String stringData = null;
                        if (rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }

                        record.addColumn(new StringColumn(stringData));
                        break;
                    case 2:
                    case 3:
                    case 6:
                    case 7:
                    case 8:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;
                    case 91:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;
                    case 92:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;
                    case 93:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;
                    case 2005:
                    case 2011:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;
                    default:
                        throw DataXException.asDataXException(HANAReaderErrorCode.UNSUPPORTED_TYPE,
                                String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .", metaData.getColumnName(i), metaData.getColumnType(i), metaData.getColumnClassName(i)));
                }
            }
        } catch (Exception var11) {
            taskPluginCollector.collectDirtyRecord(record, var11);
            if (var11 instanceof DataXException) {
                throw (DataXException)var11;
            }
        }
        return record;
    }
}
