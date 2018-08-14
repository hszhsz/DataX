package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName CassandraTaskProxy
 * @Description TODO
 * @Author heshaozhong
 * @Date 下午8:45 2018/8/13
 */
public class CassandraTaskProxy {

    private final static Logger LOG = LoggerFactory.getLogger(CassandraTaskProxy.class);

    private CassandraHelper cassandraHelper = null;

    public CassandraTaskProxy(Configuration originalConfig) {
        cassandraHelper = new CassandraHelper(originalConfig);
    }

    public void init() {
        cassandraHelper.connect();
        cassandraHelper.createKeyspace();
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector){
        Record record;
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    cassandraHelper.createTable(record);
                    cassandraHelper.insert(record);
                } catch (Exception e) {
                    taskPluginCollector.collectDirtyRecord(record, e);
                    continue;
                }
                try {
                } catch (IllegalArgumentException e) {
                    if(e.getMessage().equals("No columns to insert")){
                        LOG.info(String.format("record is empty, 您配置nullMode为[skip],将会忽略这条记录,record[%s]", record.toString()));
                        continue;
                    }else {
                        taskPluginCollector.collectDirtyRecord(record, e);
                        continue;
                    }
                }
            }
        }catch (Exception e){
            throw DataXException.asDataXException(CassandraWriterErrorCode.INSERT_CASSANDRA_ERROR,e);
        }finally {
            cassandraHelper.close();
        }
    }

    public void close() {
        cassandraHelper.close();
    }
}
