package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.datastax.driver.core.*;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/13
 */
public class CassandraReader extends Reader {
    public static class Job extends Reader.Job {
        private Configuration originConfig = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            //一个表对应一个task
            return CassandraHelper.split(adviceNumber, originConfig);
        }

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            CassandraHelper.validateConfiguration(originConfig);

        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(CassandraReader.Task.class);
        private Configuration taskConfig;
        private Cluster cluster;
        private Session session;
        // private Map<DataType, String> columnMap;
        private Gson gson = new Gson();

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            String contactPoint = taskConfig.getNecessaryValue(Key.CONTACTPOINT, CommonErrorCode.CONFIG_ERROR);
            cluster = Cluster.builder().addContactPoint(contactPoint).build();
            session = cluster.newSession();
            //  columnMap=CassandraHelper.getColumnMap(cluster,taskConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            String sql = taskConfig.getNecessaryValue(Constant.SQL, CommonErrorCode.CONFIG_ERROR);
            Record record = recordSender.createRecord();
            ResultSet resultSet;
            try {
                 resultSet = session.execute(sql);
            }catch (Exception e){
                LOG.error("Exception", e);
                return;
            }
            Iterator<Row> result = resultSet.iterator();
            while (result.hasNext()) {
                try {
                    Row row = result.next();
                    Iterator<ColumnDefinitions.Definition> it = row.getColumnDefinitions().asList().iterator();
                    while (it.hasNext()) {
                        ColumnDefinitions.Definition definition = it.next();
                        Map<String, Object> columnInfo = CassandraHelper.buildColumnInfo(row, definition);
                        if (!columnInfo.isEmpty())
                            record.addColumn(new StringColumn(gson.toJson(columnInfo)));
                    }
                } catch (Exception e) {
                    LOG.info("Exception", e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    record = recordSender.createRecord();
                    continue;
                }
                recordSender.sendToWriter(record);
                record = recordSender.createRecord();
            }
            recordSender.flush();
        }

        @Override
        public void destroy() {
            if (session != null) {
                session.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        }
    }
}
