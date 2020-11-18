package cn.ctyun.datax.plugin.reader.hanareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.util.Codecs;
import com.sap.i18n.decfloat.DecFloat;
import com.sap.i18n.decfloat.DecFloatType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * HANA DB UTIL
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public class HANADBUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HANADBUtil.class);
    private static final String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";

    /**
     * 获取HANA客户端连接
     * @param conf 配置文件类
     * @return HANA客户端连接
     */
    public static void connect(Configuration conf) {
        String jcoASHost = conf.getString(KeyConstant.JCO_ASHOST);
        String jcoClient = conf.getString(KeyConstant.JCO_CLIENT);
        String jcoUser = conf.getString(KeyConstant.JCO_USER);
        String jcoPasswd = conf.getString(KeyConstant.JCO_PASSWD);
        String jcoSysnr = conf.getString(KeyConstant.JCO_SYSNR);
        LOG.info("JCO_ASHOST:{},JCO_CLIENT:{},JCO_USER:{},JCO_PASSWD:{},JCO_SYSNR:{}",
                jcoASHost,jcoClient,jcoUser,jcoPasswd,jcoSysnr);

        Properties connectProperties = new Properties();
        //服务器
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, jcoASHost);
        //系统编号
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,  jcoSysnr);
        //SAP集团
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, jcoClient);
        //SAP用户名
        connectProperties.setProperty(DestinationDataProvider.JCO_USER,   jcoUser);
        //密码
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, jcoPasswd);
        //登录语言
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG,   "zh");
        //最大连接数
        connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "0");
        //最大连接线程
        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, "10");

        createDataFile(ABAP_AS_POOLED, "jcoDestination", connectProperties);
    }

    /**
     * 创建SAP接口属性文件。
     * @param name          ABAP管道名称
     * @param suffix        属性文件后缀
     * @param properties    属性文件内容
     */
    private static void createDataFile(String name, String suffix, Properties properties){
        File cfg = new File(name+"."+suffix);
        if(cfg.exists()){
            cfg.deleteOnExit();
        }
        try{
            FileOutputStream fos = new FileOutputStream(cfg, false);
            properties.store(fos, "for tests only !");
            fos.close();
        }catch (Exception e){
            System.out.println("Create Data file fault, error msg: " + e.toString());
            throw new RuntimeException("Unable to create the destination file " + cfg.getName(), e);
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


    public static Record transportOneRecord(RecordSender recordSender, JCoTable jCoTable, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
        LOG.info("rows:{},columns:{}",jCoTable.getNumRows(),jCoTable.getNumColumns());
        for(int i = 0; i < jCoTable.getNumRows(); i++) {
            jCoTable.setRow(i);
            Record record = buildRecord(recordSender, jCoTable, mandatoryEncoding, taskPluginCollector);
            recordSender.sendToWriter(record);
        }
        return null;
    }

    private static Record buildRecord(RecordSender recordSender, JCoTable jCoTable, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
        Record record = recordSender.createRecord();
        JCoMetaData metaData = jCoTable.getMetaData();
        try {
            String[] values = jCoTable.getValue(0).toString().split("\t");

            for(int i = 0; i < values.length; i++) {
                record.addColumn(new StringColumn(values[i]));
//                switch(metaData.getType(i)) {
//                    case 0:
//                    case 1:
//                    case 2:
//                    case 3:
//                    case 4:
//                    case 6:
//                        record.addColumn(new StringColumn(values[i]));
//                        break;
//                    case 7:
//                        record.addColumn(new LongColumn(values[i]));
//                        break;
//                    case 8:
//                    case 9:
//                    case 10:
//                        record.addColumn(new LongColumn(values[i]));
//                        break;
//                    case 17:
//                    case 99:
//                        record.addColumn(new StringColumn(values[i]));
//                        break;
//                    case 23:
//                    case 24:
//                        record.addColumn(new DoubleColumn(values[i]));
//                        break;
//                    case 29:
//                        record.addColumn(new StringColumn(values[i]));
//                        break;
//                    case 30:
//                        record.addColumn(new StringColumn(values[i]));
//                        break;
//                    default:
//                        record.addColumn(new StringColumn(values[i]));
//                        break;                }
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
