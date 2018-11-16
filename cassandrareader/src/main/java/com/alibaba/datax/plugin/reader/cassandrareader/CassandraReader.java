package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

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
            CassandraHelper.init(originConfig);
        }

        @Override
        public void destroy() {
            CassandraHelper.close();
        }
    }

    public static class Task extends Reader.Task {

        private Configuration taskConfig;
        private CassandraReaderProxy proxy;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.proxy = new CassandraReaderProxy(taskConfig);
        }


        @Override
        public void prepare() {
            proxy.init();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            proxy.startRead(recordSender, this.getTaskPluginCollector());

        }

        @Override
        public void destroy() {
            if (proxy != null)
                proxy.close();
        }
    }
}
