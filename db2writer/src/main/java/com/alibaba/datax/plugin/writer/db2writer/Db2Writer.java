package com.alibaba.datax.plugin.writer.db2writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.util.List;


public class Db2Writer extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.DB2;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck() {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver() {
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }

    }
//
//    private static Connection getAliUserDB2Connection() throws SQLException {
//        return DriverManager.getConnection("jdbc:db2://10.60.2.241:50000/NEWTODS", "db2inst1", "anywhere");
//    }
//
//    public static void main(String[] args) throws SQLException {
//        List<String> cols=Arrays.asList("CUST_ID",
//                "APPLY_STATUS",
//                "LICENSE_CODE",
//                "NATION_CUST_CODE",
//                "CUST_NAME",
//                "CUST_SHORT_NAME",
//                "IS_BUSILICE",
//                "BUSI_LICENSE_ID",
//                "MANAGER",
//                "BUSI_ADDR",
//                "STATUS",
//                "CHARTER_SCOPE",
//                "BUSI_AREA_PART",
//                "BUSI_ADDR_BEGIN_DATE",
//                "BUSI_ADDR_AVAIL_DATE",
//                "MREGIE_ID",
//                "CREGIE_ID",
//                "TEND_ID_CARD",
//                "BUSI_ADDR",
//                "ORDER_TEL",
//                "REG_FUND",
//                "ALL_BUSI_AREA",
//                "CUST_TYPE2",
//                "IDENTITY_CARD_ID",
//                "MD_SYNC_AT");
//        Triple<List<String>, List<Integer>, List<String>> res = DBUtil.getColumnMetaData(getAliUserDB2Connection(), "ALIUSER.LW_AL_CUST", StringUtils.join(cols,','));
////        Triple<List<String>, List<Integer>, List<String>> res = DBUtil.getColumnMetaData(getAliUserDB2Connection(), "ALIUSER.LW_AL_CUST", "*");
//        System.out.println(res);
//    }


}
