package com.ys.day0714

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by ys on 16-7-14.
  */
object FetchKafkaDataAndCompute {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("FetchKafkaDataAndCompute").setMaster("local[*]")
    val ssc=new StreamingContext(conf,Seconds(10))
    val km=Map(
    "bootstrap.servers"-> "localhost:9092",
      "group.id"->"test",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms"-> "1000",
      "session.timeout.ms"-> "30000",
      "session.timeout.ms"->"30000",
      "key.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
    )
    /*
    Properties props = new Properties();
     props.put("bootstrap.servers", "localhost:9092");
     props.put("group.id", "test");
     props.put("enable.auto.commit", "true");
     props.put("auto.commit.interval.ms", "1000");
     props.put("session.timeout.ms", "30000");
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
     consumer.subscribe(Arrays.asList("foo", "bar"));
     while (true) {
         ConsumerRecords<String, String> records = consumer.poll(100);
         for (ConsumerRecord<String, String> record : records)
             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
     }
     */
    val lines=KafkaUtils.createDirectStream(ssc,km)
  }
}
