package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.util.Configuration;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/4/27 16:25
 **/
public class RocketmqUtil {

    private static final Logger log = LoggerFactory.getLogger(RocketmqUtil.class);
    private final static String WRITE_HOST = "host";
    private final static String WRITE_PORT = "port";
    private final static String WRITE_USER = "username";
    private final static String WRITE_PWD = "password";
    private final static String WRITE_GROUP = "groupName";
    private final static String WRITE_TOPIC = "topic";
    private RocketmqUtil(){}

    public DefaultMQProducer getProducer(Configuration conf){
        //创建一个新的连接
        Connection connection = null;

            log.info("当前配置信息如下：【host:{}，port:{}，username:{}，password:{}，groupName:{}】",
                    conf.getString(WRITE_HOST),
                    conf.getString(WRITE_PORT),
                    conf.getString(WRITE_USER),
                    conf.getString(WRITE_PWD),
                    conf.getString(WRITE_GROUP)
                 );
            // 创建连接工厂
            // 实例化消息生产者Producer
            DefaultMQProducer producer = new DefaultMQProducer(conf.getString(WRITE_GROUP));
            // 设置NameServer的地址
            producer.setNamesrvAddr(conf.getString(WRITE_HOST)+":"+conf.getString(WRITE_PORT));
            producer.setCreateTopicKey(conf.getString(WRITE_TOPIC));
            // 启动Producer实例
            return producer;
    }



    public void closeConn(DefaultMQProducer producer){
        producer.shutdown();
    }

//    public void send(String data,Configuration conf){
//        try {
//            Connection connection=getConn(conf);
//            Channel channel =getChannel(connection,conf);
//            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().deliveryMode(2).
//                    contentEncoding("UTF-8").build();
//            channel.basicPublish(conf.getString("exchangeName"),conf.getString("routeKey"),properties,data.getBytes());
//            closeConn(connection,channel);
//        } catch (IOException e) {
//            log.error("发送消息失败：{}",e.getMessage());
//        }
//    }
//
//    public void send(String data,Configuration conf,Connection conn,Channel channel){
//        try {
//            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().deliveryMode(2).
//                    contentEncoding("UTF-8").build();
//            channel.basicPublish(conf.getString("exchangeName"),conf.getString("routeKey"),properties,data.getBytes());
//            closeConn(conn,channel);
//        } catch (IOException e) {
//            log.error("发送消息失败：{}",e.getMessage());
//        }
//    }

    public static RocketmqUtil getInstance(){
        return RocketmqUtilHandler.instance;
    }

    private static class RocketmqUtilHandler{
        public static RocketmqUtil instance=new RocketmqUtil();
    }

}
