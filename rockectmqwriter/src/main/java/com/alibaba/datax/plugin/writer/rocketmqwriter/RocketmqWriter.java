package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/4/27 16:18
 **/
public class RocketmqWriter extends Writer {

    private final static String WRITE_COLUMNS = "column";
    private final static String WRITE_TOPIC = "topic";
    private final static String WRITE_TAG = "tag";
    private final static String WRITE_CDP_TAG = "cdpTag";

    private final static int BATCH_SIZE = 1000;

    private final static int SEND_SIZE = 16;

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;

        private List<String> columnList = null;

        private Connection conn = null;
        private DefaultMQProducer producer= null;


        private int count=0;

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void prepare() {
//            this.conf = super.getPluginJobConf();
//            this.conn = RabbitmqUtil.getInstance().getConn(conf);
//            if (this.conn != null) {
//                this.channel = RabbitmqUtil.getInstance().getChannel(conn, conf);
//            }
//            check(this.conn,this.channel);
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            if (this.conn == null) {
                this.producer = RocketmqUtil.getInstance().getProducer(conf);
                try {
                    producer.start();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }
            columnList = this.conf.getList(WRITE_COLUMNS, String.class);
           // columnList=JSONArray.parseArray(this.conf.getString(WRITE_COLUMNS), RocketmqColumn.class);

            log.info("配置：{}  列信息：",JSONObject.toJSONString(conf),this.conf.getString(WRITE_COLUMNS));
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<>(BATCH_SIZE);
            Record record;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= BATCH_SIZE) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }
            if (!writerBuffer.isEmpty()) {
                log.info("本次需要处理的数据大小：{}",writerBuffer.size());
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }
            String msg = String.format("task end, write size :%d ，msg count：%d", total,count);
            getTaskPluginCollector().collectMessage("writesize", String.valueOf(total));
            log.info(msg);
        }

        private long doBatchInsert(final List<Record> writerBuffer) {
            int index = 0;
            try {
                List<Message> dataList= Lists.newArrayList();
                log.info("本次批量处理数据数：{},当前第",writerBuffer.size());
                for(Record record:writerBuffer){
                    String msgContent=null;
                    if(Objects.nonNull(conf.getString(WRITE_CDP_TAG)) &&
                            StringUtils.isNotBlank(conf.getString(WRITE_CDP_TAG))){
                        List<String> cdpTagList = this.conf.getList(WRITE_CDP_TAG, String.class);
                        String value = cdpTagList.get(0);
                        String cdpTag=new String(Base64.getDecoder().decode(value), "UTF-8");
                        Column userIdColumn = record.getColumn(0);
                        Column codeValueColumn = record.getColumn(2);
                        Column codeValueIdColumn = record.getColumn(3);
                        log.info("cdp tag ********* {} {}",record,cdpTag);
                        String result=String.format(cdpTag,userIdColumn.getRawData(),codeValueColumn.getRawData(),codeValueIdColumn.getRawData());
                        cdpTagList=Lists.newArrayList();
                        cdpTagList.add(result);
                        log.info("result ********* {}",cdpTagList);
                        msgContent=cdpTagList.toString();
                    }else{
                        int length = record.getColumnNumber();
                        for (int i = 0; i < length; i++) {
                            JSONObject jsonObject=new JSONObject();
                            Column column=record.getColumn(i);
                            jsonObject.put(columnList.get(i),column.getRawData());
                            msgContent=jsonObject.toJSONString();
                        }
                    }
                    if(Objects.nonNull(msgContent)){
                        Message msg = new Message(conf.getString(WRITE_TOPIC) /* Topic */,
                                conf.getString(WRITE_TAG) /* Tag */,
                                msgContent.getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                        );
                        dataList.add(msg);
                        index++;
                    }
                    if(index%10==0){
                        sendMsg(dataList);
                    }
                }

                if(!dataList.isEmpty()){
                    sendMsg(dataList);
                }
            } catch (Exception e) {
                log.error(e.getMessage());
                throw DataXException.asDataXException(RocketmqWriterErrorCode.EXECUTE_ERROR, e);
            }
            return index;
        }

        private void sendMsg(List<Message> dataList) throws InterruptedException, RemotingException,
                MQClientException, MQBrokerException, UnsupportedEncodingException {

            // 发送消息到一个Broker
            producer.send(dataList);
            dataList.clear();
            count++;
        }

        @Override
        public void destroy() {
            RocketmqUtil.getInstance().closeConn(producer);
        }
//        public static void main(String[] args) throws Exception {
//            // 实例化消息生产者Producer
//            DefaultMQProducer producer = new DefaultMQProducer("cdp_test_123");
//            // 设置NameServer的地址
//            producer.setNamesrvAddr("rocketmq-namesrv.group-addon-rocketmq--e8c7c8905faf24262b854c9dee3306b9f.svc.cluster.local:9876");
//            // 启动Producer实例
//            producer.start();
//            for (int i = 0; i < 100; i++) {
//                // 创建消息，并指定Topic，Tag和消息体
//                Message msg = new Message("CDP_TAG_CHANGED" /* Topic */,
//                        "TagA" /* Tag */,
//                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//                );
//                // 发送消息到一个Broker
//                SendResult sendResult = producer.send(msg);
//                // 通过sendResult返回消息是否成功送达
//                System.out.printf("%s%n", sendResult);
//            }
//            // 如果不再发送消息，关闭Producer实例。
//            producer.shutdown();
//        }

    }

}
