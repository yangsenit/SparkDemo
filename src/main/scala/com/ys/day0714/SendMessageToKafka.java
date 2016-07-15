package com.ys.day0714;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by ys on 16-7-14.
 */
public class SendMessageToKafka {
    public static void main(String [] args){
        /*
        https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
        这是编写kafka java程序的例子
        ys@master1:~/installed_soft/kafka_2.10-0.10.0.0/bin$ ./kafka-topics.sh --create --zookeeper master1:2181,slave1:2181,slave2:2181 --replication-factor 1 --partitions 1 --topic my-topic
        我们创建了 my-topic

        kafka中的 producer topic consumer  多个topic可以是属于一个组group组
        producer 通过kafka的配置 创建producer  通过send的方法 发送消息到 topic中
        topic 是要自己通过 kafka的命令行提前 创建好的
        consumer 通过kafka的配置文件 创建consumer

         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "master1:9092,slave1:9092,slave2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        while(true) {
            String key = java.util.UUID.randomUUID().toString();//获得唯一表示作为key
            String value = new CreateLog1().create();
            // 我们需要事先在kafka创建一个topic my-topic
            try{
                Thread.currentThread().sleep(500);
            }catch (Exception e){
                System.out.print(e.getStackTrace());
            }
            producer.send(new ProducerRecord<String, String>("my-topic",key,value));
        }
    }
}
//专门负责产生数据
/*
数据的格式如下：
data：日期，格式为 yyyy-MM-dd
timeStamp：时间戳
userId：用户ID
pageID:页面ID
chanelID：板块的ID
action：点击和注册

每一条记录是作为<k,v>中的value 因为kafka中的数据是以<K，V>的格式存储的

实现了向kafka 中的 my-topic 中实时的 发送消息，
现在我们要做的就是通过 spark streaming 实时的读取 kafka中的数据，然后，然后通过Streaming的实时计算获得我们
想要的数据，其实这这的在模拟这样实验环境，实时的读取 最新的评论，获得评论的信息等等

 */
class CreateLog implements Runnable{
    public static String [] channels={"hadoop","spark","flume","kafka","impala","HBase","ML"};
    public static String [] actions={"view","register"};
    private String data;
    private String timeStamp;
    private String userID;
    private String pageID;
    private String channelID;
    private String action;
    private String userLog;
    public String getUserLog() {
        return userLog;
    }
    public void run() {
        while(true){
            data=new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            timeStamp=String.valueOf(new Date().getTime());
            userID=String.valueOf(new Random().nextInt(100000));
            pageID=String.valueOf(new Random().nextInt(20));
            channelID=channels[new Random().nextInt(channels.length)];
            action=actions[new Random().nextInt(actions.length)];
            userLog=data+"\t"+
                    timeStamp+"\t"+
                    userID+"\t"+
                    pageID+"\t"+
                    channelID+"\t"+
                    action+"\t";
//System.out.println(userLog);
        }
    }
}
class CreateLog1 {
    public static String [] channels={"hadoop","spark","flume","kafka","impala","HBase","ML"};
    public static String [] actions={"view","register"};
    public String create() {
        String data=new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String timeStamp=String.valueOf(new Date().getTime());
        String userID=String.valueOf(new Random().nextInt(100000));
        String  pageID=String.valueOf(new Random().nextInt(20));
        String channelID=channels[new Random().nextInt(channels.length)];
        String action=actions[new Random().nextInt(actions.length)];
        String userLog=data+"\t"+
                timeStamp+"\t"+
                userID+"\t"+
                pageID+"\t"+
                channelID+"\t"+
                action+"\t";
        System.out.println(userLog);
        return userLog;
    }
}
