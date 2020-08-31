package cn.ctyun.datax.plugin.reader.hanareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * HANA reader插件
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public class HANAReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            if (this.originalConfig.getInt(KeyConstant.FETCH_SIZE) != null) {
                LOG.warn("对 hanareader 不需要配置 fetchSize, hanareader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
            }
            int fetchSize = 2000;
            this.originalConfig.set(KeyConstant.FETCH_SIZE, fetchSize);
        }

        @Override
        public void preCheck(){
            int fetchSize = this.originalConfig.getInt(KeyConstant.FETCH_SIZE);
            List<Object> connList = this.originalConfig.getList(KeyConstant.CONN_MARK, Object.class);
            Configuration connConf = Configuration.from(connList.get(0).toString());

            String querySql = connConf.getList("querySql",Object.class).get(0).toString();
            Connection conn = HANADBUtil.connect(this.originalConfig);
            try {
                HANADBUtil.query(conn, querySql, fetchSize);

            } catch (Exception var20) {
                throw DataXException.asDataXException(HANAReaderErrorCode.SQL_EXECUTE_FAIL, String.format("SQL执行失败[%s]", querySql), var20);
            } finally {
                HANADBUtil.closeDBResources(null, conn);
            }
        }

        /**
         * 只允许配置一条sql，且分片数始终为1
         * @param adviceNumber 未使用
         * @return 分片数
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> splitConfigs = new ArrayList<>();
            splitConfigs.add(originalConfig);
            return splitConfigs;
        }

        @Override
        public void post() { }

        @Override
        public void destroy() { }

    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(HANAReader.Task.class);
        private Configuration readerSliceConfig;
        private int taskGroupId = super.getTaskGroupId();
        private int taskId = super.getTaskId();
        private String basicMsg;
        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.basicMsg = String.format("%s:[%s]", KeyConstant.JDBC_URL,
                    readerSliceConfig.getString(KeyConstant.JDBC_URL));
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(KeyConstant.FETCH_SIZE);
            String querySql = readerSliceConfig.getString("querySql");
            LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, this.basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(this.taskGroupId, this.taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            Connection conn = HANADBUtil.connect(this.readerSliceConfig);
            try {
                ResultSet rs = HANADBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnNumber = metaData.getColumnCount();
                PerfRecord allResultPerfRecord = new PerfRecord(this.taskGroupId, this.taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();
                long rsNextUsedTime = 0L;
                for(long lastTime = System.nanoTime(); rs.next(); lastTime = System.nanoTime()) {
                    rsNextUsedTime += System.nanoTime() - lastTime;
                    final String mandatoryEncoding = readerSliceConfig.getString(KeyConstant.MANDATORY_ENCODING, "");
                    HANADBUtil.transportOneRecord(recordSender, rs, metaData, columnNumber,
                            mandatoryEncoding, super.getTaskPluginCollector());
                }
                allResultPerfRecord.end(rsNextUsedTime);
                LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, this.basicMsg);
            } catch (Exception var20) {
                throw DataXException.asDataXException(HANAReaderErrorCode.SQL_EXECUTE_FAIL, String.format("SQL执行失败[%s]", querySql), var20);
            } finally {
                HANADBUtil.closeDBResources(null, conn);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

}
