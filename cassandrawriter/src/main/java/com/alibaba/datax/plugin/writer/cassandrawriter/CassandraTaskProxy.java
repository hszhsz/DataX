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

    private Long timer = null;
    private int duration;
    private int batchSize;
    private Configuration configuration;
//    private Boolean needCreateTable;
    private List<Object> column = null;

    public CassandraTaskProxy(Configuration originalConfig) {
        batchSize = originalConfig.getInt(Constants.BATCH_SIZE, 1);
        duration = originalConfig.getInt(Constants.DURATION, 1);
        configuration = originalConfig;
//        needCreateTable = originalConfig.getBool(Constants.CREATETABLE,false);
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
                if (CassandraHelper.needCreateTable) {
                    LOG.info("needCreateTable========:" + CassandraHelper.needCreateTable);
                    CassandraHelper.createTable(record);
                    CassandraHelper.needCreateTable = false;
                    CassandraHelper.initTableMeta(); //cassandraHelper 需要重新初始化tablemeta info
                }
                if(batchSize==1){
                    try {
                        insert(record,taskPluginCollector);
                    }catch (Exception e){
                        LOG.error(String.format("insert error ,record[%s]", record.toString()));
                        taskPluginCollector.collectDirtyRecord(record, e);
                    }
                }else {
                    recordList.add(record);
                    try {
                        if (recordList.size() >= batchSize || System.currentTimeMillis() - timer > duration * 1000) {
                            CassandraHelper.insertBatch(recordList,taskPluginCollector);
                            recordList.clear();
                            timer = System.currentTimeMillis();
                        }
                        //cassandraHelper.insert(record);
                    } catch (Exception e) {
                        LOG.error(String.format("error occurs,error[%s]", e.getMessage()));
                        recordList.forEach(x-> taskPluginCollector.collectDirtyRecord(x, e));
                        taskPluginCollector.collectDirtyRecord(record, e);
                    }
                }
            }
            if(!recordList.isEmpty()) {
                CassandraHelper.insertBatch(recordList,taskPluginCollector);
                recordList.clear();
            }
        } catch (Exception e) {

            throw DataXException.asDataXException(CassandraWriterErrorCode.INSERT_CASSANDRA_ERROR, e);

        } finally {
            if (!recordList.isEmpty()) {
                CassandraHelper.insertBatch(recordList,taskPluginCollector);
                recordList.clear();
                timer = System.currentTimeMillis();
            }
        }
    }

    public void close() {}

    public void insert(Record record,TaskPluginCollector taskPluginCollector)
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
            if(column.getRawData()==null){
                sb.append(column.getRawData());
            }else {
                switch (column.getType()) {
                    case INT:
                        sb.append(column.asBigInteger());
                        break;
                    case BOOL:
                        sb.append(column.asBoolean());
                        break;
                    case DATE:
                        sb.append(column.asLong());
                        break;
                    case LONG:
                        sb.append(column.asLong());
                        break;
                    case BYTES:
                        if (column.asBytes() == null) sb.append(column.asBytes());
                        else sb.append("'").append(column.asBytes().toString()).append("'");
                        break;
                    case DOUBLE:
                        sb.append(column.asDouble());
                        break;
                    case STRING:
                        if (column.asString() == null) sb.append(column.asString());
                        else sb.append("'").append(column.asString().replace("'","" ).replace("\"","" ).replace(","," ")).append("'");
                        break;
                    case NULL:
                    case BAD:
                        break;
                    default:
                }
            }
            if(j != (record.getColumnNumber() -1)) {
                sb.append(",");
            }
        }

        sb.append(" )");
        try {
            CassandraHelper.insert(sb.toString());
        }catch (Exception e){
            taskPluginCollector.collectDirtyRecord(record,e);
            LOG.error("insert error sql {}, error {}",sb.toString(),e.getMessage());
            throw  e;
        }
    }


}
