package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/15
 */
public class CassandraReaderProxy {
    private static Logger LOG = LoggerFactory.getLogger(CassandraReaderProxy.class);

    private Configuration taskConfig;
    private Cluster cluster;
    private Session session;

    public CassandraReaderProxy(Configuration taskConfig) {
        this.taskConfig = taskConfig;
    }

    public void init() {
        cluster = CassandraHelper.initCluster(taskConfig);//Cluster.builder().addContactPoint(contactPoint).build();
        session = cluster.newSession();
    }

    public void startRead(RecordSender recordSender, TaskPluginCollector collector) {

        String sql = taskConfig.getNecessaryValue(Constants.SQL, CommonErrorCode.CONFIG_ERROR);
        ResultSet resultSet;
        try {
            resultSet = session.execute(sql);
        } catch (Exception e) {
            LOG.error("Exception", e);
            return;
        } finally {
            close();
        }
        Map<String, Object> tableInfoFromSql = CassandraHelper.splitSQL(sql);
        Map<String, Object> tableInfo = new HashMap<>(2);
        tableInfo.put(Constants.KETSPACE, tableInfoFromSql.get(Constants.KETSPACE));
        tableInfo.put(Constants.TABLE, tableInfoFromSql.get(Constants.TABLE));
        String mode = taskConfig.getString(Key.MODE, Constants.ALL_MODE);
        Iterator<Row> result = resultSet.iterator();
        Record record = recordSender.createRecord();
        while (result.hasNext()) {
            try {
                Row row = result.next();
                if (mode.equals(Constants.ALL_MODE)) {
                    buildRecordWithAllMode(row, record, tableInfo);
                } else {
                    buildRecordWithNormalMode(row, record);
                }

            } catch (Exception e) {
                LOG.error("Exception", e);
                collector.collectDirtyRecord(record, e);
                record = recordSender.createRecord();
                continue;
            }
            recordSender.sendToWriter(record);
            record = recordSender.createRecord();
        }
        recordSender.flush();

    }

    private void buildRecordWithNormalMode(Row row, Record record) {
        Iterator<ColumnDefinitions.Definition> it = row.getColumnDefinitions().asList().iterator();
        while (it.hasNext()) {
            ColumnDefinitions.Definition definition = it.next();
            String columnName = definition.getName();
            switch (definition.getType().getName()) {
                case INT:
                    Integer valueInt = row.get(columnName, TypeCodec.cint());
                    record.addColumn(new LongColumn(valueInt));
                    break;
                case SMALLINT:
                    Short valueSMALLINT = row.get(columnName, TypeCodec.smallInt());
                    record.addColumn(new LongColumn(null == valueSMALLINT ? null : valueSMALLINT.longValue()));
                    break;
                case TINYINT:
                    Byte valueTINYINT = row.get(columnName, TypeCodec.tinyInt());
                    record.addColumn(new LongColumn(null == valueTINYINT ? null : valueTINYINT.intValue()));
                    break;
                case VARINT:
                    BigInteger valueVARINT = row.get(columnName, TypeCodec.varint());
                    record.addColumn(new LongColumn(null == valueVARINT ? null : valueVARINT.longValue()));
                    break;
                case TIME:
                    Long valueTIME = row.get(columnName, TypeCodec.time());
                    record.addColumn(new LongColumn(valueTIME));
                    break;
                case COUNTER:
                    Long counter = row.get(columnName, TypeCodec.counter());
                    record.addColumn(new LongColumn(counter));
                    break;
                case ASCII:
                    String valueASCII = row.get(columnName, TypeCodec.ascii());
                    record.addColumn(new StringColumn(valueASCII));
                    break;

                case BIGINT:
                    Long valueBIGINT = row.get(columnName, TypeCodec.bigint());
                    record.addColumn(new LongColumn(valueBIGINT));
                    break;
                case BLOB:
                    ByteBuffer valueBLOB = row.get(columnName, TypeCodec.blob());
                    record.addColumn(new BytesColumn(null == valueBLOB ? null : valueBLOB.array()));
                    break;
                case BOOLEAN:
                    Boolean valueBOOLEAN = row.get(columnName, TypeCodec.cboolean());
                    record.addColumn(new BoolColumn(valueBOOLEAN));
                    break;
                case DECIMAL:
                    BigDecimal valueDECIMAL = row.get(columnName, TypeCodec.decimal());
                    record.addColumn(new StringColumn(null == valueDECIMAL ? null : valueDECIMAL.toString()));
                    break;
                case DOUBLE:
                    Double valueDOUBLE = row.get(columnName, TypeCodec.cdouble());
                    record.addColumn(new DoubleColumn(valueDOUBLE));
                    break;
                case FLOAT:
                    Float valueFLOAT = row.get(columnName, TypeCodec.cfloat());
                    record.addColumn(new DoubleColumn(valueFLOAT == null ? null : valueFLOAT.doubleValue()));
                    break;
                case VARCHAR:
                    String valueVARCHAR = row.get(columnName, TypeCodec.varchar());
                    record.addColumn(new StringColumn(valueVARCHAR));
                    break;
                case TEXT:
                    String valueTEXT = row.get(columnName, TypeCodec.varchar());
                    record.addColumn(new StringColumn(valueTEXT));
                    break;
                case TIMESTAMP:
                    Date valueTIMESTAMP = row.get(columnName, TypeCodec.timestamp());
                    record.addColumn(new DateColumn(valueTIMESTAMP));
                    break;
                case DATE:
                    LocalDate valueDATE = row.get(columnName, TypeCodec.date());
                    record.addColumn(new DateColumn(valueDATE.getMillisSinceEpoch()));
                    break;
                case INET:
                case TIMEUUID:
                case CUSTOM:
                case UUID:
                case DURATION:
                case LIST:
                case MAP:
                case SET:
                case UDT:
                case TUPLE:
                    Object value = row.getObject(columnName);
                    record.addColumn(new StringColumn(JSON.toJSON(value).toString()));
                    break;
            }

        }

    }

    private void buildRecordWithAllMode(Row row, Record record, Map<String, Object> tableInfo) {
        record.addColumn(new StringColumn(JSON.toJSON(tableInfo).toString()));
        Iterator<ColumnDefinitions.Definition> it = row.getColumnDefinitions().asList().iterator();
        int index = 1;
        while (it.hasNext()) {
            ColumnDefinitions.Definition definition = it.next();
            Map<String, Object> columnInfo = CassandraHelper.buildColumnInfo(row, definition);
            // if (!columnInfo.isEmpty()) {有的时候列名包含值，列值为空
            columnInfo.put(Constants.INDEX, index);
            record.addColumn(new StringColumn(JSON.toJSON(columnInfo).toString()));
            // }
            index = index + 1;
        }
    }

    public void close() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }
}
