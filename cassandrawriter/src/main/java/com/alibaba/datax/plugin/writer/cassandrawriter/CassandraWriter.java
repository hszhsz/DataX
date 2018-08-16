package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName CassandraWriter
 * @Description TODO
 * @Author heshaozhong
 * @Date 下午7:35 2018/8/13
 */
public class CassandraWriter extends Writer {
    public static class Job extends Writer.Job {

        private Configuration originConfig = null;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            CassandraHelper.validateParameter(originConfig);
        }

        @Override
        public void prepare() {
            CassandraHelper.prepare(this.originConfig);
            Boolean truncate = originConfig.getBool(Constants.TRUNCATE, false);
            if (truncate) {
                CassandraHelper.truncateTable(this.originConfig);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy() {
            // NOOP
        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private CassandraTaskProxy cassandraTaskProxy;

        @Override
        public void init() {
            taskConfig = super.getPluginJobConf();
            cassandraTaskProxy = new CassandraTaskProxy(taskConfig);
            cassandraTaskProxy.init();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            cassandraTaskProxy.startWriter(lineReceiver, super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            if (null != cassandraTaskProxy) {
                cassandraTaskProxy.close();
            }
        }
    }
}
