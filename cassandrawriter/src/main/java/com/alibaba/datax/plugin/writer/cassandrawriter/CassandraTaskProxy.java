package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName CassandraTaskProxy
 * @Description TODO
 * @Author heshaozhong
 * @Date 下午8:45 2018/8/13
 */
public class CassandraTaskProxy {

    private final static Logger LOG = LoggerFactory.getLogger(CassandraTaskProxy.class);

    private CassandraHelper cassandraHelper = null;
    private Long timer = null;
    private int duration;
    private int batchSize;
    private Configuration configuration;
    private Boolean needCreateTable;
    private List<Object> column = null;

    public CassandraTaskProxy(Configuration originalConfig) {
        cassandraHelper = new CassandraHelper(originalConfig);
        batchSize = originalConfig.getInt(Constants.BATCH_SIZE, 1);
        duration = originalConfig.getInt(Constants.DURATION, 1);
        configuration = originalConfig;
        needCreateTable = originalConfig.getBool(Constants.CREATETABLE,false);
        column=configuration.getList(Constants.COLUMN);
    }

    public void init() {
        timer = System.currentTimeMillis();
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        Record record;
        List<Record> recordList = new ArrayList<Record>(batchSize);
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                if (needCreateTable) {
                    cassandraHelper.createTable(record);
                    needCreateTable = false;
                    cassandraHelper.initTableMeta(); //cassandraHelper 需要重新初始化tablemeta info
                }
                if(batchSize==1){
                    insert(record);
                }else {
                    recordList.add(record);
                    try {
                        // cassandraHelper.createTable(record);
                        if (recordList.size() >= batchSize || System.currentTimeMillis() - timer > duration * 1000) {
                            cassandraHelper.insertBatch(recordList);
                            recordList.clear();
                            timer = System.currentTimeMillis();
                        }
                        //cassandraHelper.insert(record);
                    } catch (Exception e) {
                        LOG.error(String.format("record is empty, 您配置nullMode为[skip],将会忽略这条记录,record[%s]", record.toString()));
                        taskPluginCollector.collectDirtyRecord(record, e);
                    }
                }
            }
            if(!recordList.isEmpty()) {
                cassandraHelper.insertBatch(recordList);
                recordList.clear();
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(CassandraWriterErrorCode.INSERT_CASSANDRA_ERROR, e);
        } finally {
            if (!recordList.isEmpty()) {
                cassandraHelper.insertBatch(recordList);
                recordList.clear();
                timer = System.currentTimeMillis();
            }
            cassandraHelper.close();
        }
    }

    public void close() {
        cassandraHelper.close();
    }
    public void insert(Record record)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ")
                .append((String) configuration.getMap(Constants.KEYSPACE).get(Constants.KEYSPACE_NAME))
                .append(".")
                .append(configuration.getString(Constants.TABLE))
                .append(" (");

        for(int i = 0; i < column.size(); i++ ) {
            sb.append(column.get(i));
            if(i != (column.size() -1)) {
                sb.append(",");
            }
        }

        sb.append(") VALUES ( ");

        for(int j = 0; j< record.getColumnNumber(); j++ ) {
            Column column = record.getColumn(j);
            switch (column.getType()) {
                case INT: sb.append(column.asBigInteger()); break;
                case BOOL: sb.append(column.asBoolean()); break;
                case DATE: sb.append(column.asDate()); break;
                case LONG: sb.append(column.asLong()); break;
                case BYTES: sb.append("'").append(column.asBytes().toString()).append("'"); break;
                case DOUBLE: sb.append(column.asDouble()); break;
                case STRING: sb.append("'").append(column.asString()).append("'"); break;
                case NULL:
                case BAD: break;
            }
            if(j != (record.getColumnNumber() -1)) {
                sb.append(",");
            }
        }

        sb.append(" )");

        cassandraHelper.insert(sb.toString());
    }


}
