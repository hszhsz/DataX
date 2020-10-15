package cn.ctyun.datax.plugin.reader.hanareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import com.sap.conn.jco.*;
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
    private static final String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private String tableName;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            if (this.originalConfig.getInt(KeyConstant.FETCH_SIZE) != null) {
                LOG.warn("对 hanareader 不需要配置 fetchSize, hanareader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
            }
            int fetchSize = 2000;
            this.originalConfig.set(KeyConstant.FETCH_SIZE, fetchSize);

            List<Object> connList = originalConfig.getList(KeyConstant.CONN_MARK, Object.class);
            Configuration connConf = Configuration.from(connList.get(0).toString());

            tableName = connConf.getString(KeyConstant.TABLE);
        }

        @Override
        public void preCheck(){
            HANADBUtil.connect(originalConfig);
        }

        /**
         * 分片数始终为1
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
        private String tableName;
        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.basicMsg = String.format("%s:[%s]", KeyConstant.JDBC_URL,
                    readerSliceConfig.getString(KeyConstant.JDBC_URL));

            tableName = readerSliceConfig.getString(KeyConstant.TABLE);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
                JCoFunction function = destination.getRepository().getFunction("BAPI_" + tableName);//从对象仓库中获取 RFM 函数：获取公司列表
                if (function == null)
                    throw new RuntimeException("BAPI_" + tableName + " not found in SAP.");
                try {
                    function.execute(destination);
                } catch (AbapException e) {
                    e.printStackTrace();
                    return;
                }
                final String mandatoryEncoding = readerSliceConfig.getString(KeyConstant.MANDATORY_ENCODING, "");

                JCoTable codes = function.getTableParameterList().getTable(tableName);
                for (int i = 0; i < codes.getNumRows(); i++) {
                    HANADBUtil.transportOneRecord(recordSender,codes,mandatoryEncoding,super.getTaskPluginCollector());
                }
            } catch (Exception e) {
                e.printStackTrace();
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
