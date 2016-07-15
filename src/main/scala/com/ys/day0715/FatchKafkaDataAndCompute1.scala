package com.ys.day0715

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
  * Created by ys on 16-7-14.
  */
object FetchKafkaDataAndCompute {
  def main(args: Array[String]) {
    // args 是个数组，数组中的每一个元素都是 String
    val conf = new SparkConf().setAppName("FetchKafkaDataAndCompute").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("/home/ys/checkpoint/kafka")

    val kafkaParameters=Map("metadata.broker.list"->"master1:9092,slave1:9092,slave2:9092")
    val topic =Set[String]("my-topic")
    //只想说一点的是  这里的泛型必须加上 ，必须加上，要不根本就没有法子去解析，kafka中的数据是什么类型，给了一StringDecoder的方式去 解析获得key和value
    //[String,String,StringDecoder, StringDecoder] 告诉spark这样解析 进来的数据是key 是String类型 value也是String类型
    // 解析的时候 key用String的方式解析 value用String的方式解析
    //泛型的一定不能掉
    val lines= KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParameters,topic).map(_._2)
//    val lines1=lines.transform(rdds=>rdds.map(x=>x._2.toString))
//    val words=lines1.transform(rdds=>rdds.flatMap(_.split("\t")))
    //val words = lines.map(_.2).flatMap{_.split(" ")}

    val words=lines.flatMap(_.split("\t"))
    val pairs = words.map((_,1))
    val wordsCount = pairs.reduceByKey(_+_)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()

    /*
15:32:00.199 [streaming-job-executor-0] INFO  o.a.spark.scheduler.DAGScheduler - Job 3 finished: print at FatchKafkaDataAndCompute.scala:35, took 0.020173 s
-------------------------------------------
Time: 1468567920000 ms
-------------------------------------------
(1468567914601,1)
(4,1)
(19,1)
(69704,1)
(15,1)
(1468567913259,1)
(2016-07-15,12)
(71189,1)
(1468567917608,1)
(1468567916105,1)
...

15:32:00.200 [JobScheduler] INFO  o.a.s.s.scheduler.JobScheduler - Finished job streaming job 1468567920000 ms.0 from job set of time 1468567920000 ms
     */
  }
}

/*
//创建Kafka元数据，来让Spark Streaming这个kafka Consumer利用
    val kafkaParameters= new HashMap[String,String]()
    kafkaParameters.put("metadata.broker.list","Master:9092,Worker1:9092")
    val topic = new HashSet[String]()
    topic.add("HelloKafka")

    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](jsc,kafkaParameters.toMap,topic.toSet).map(_._2)
  //  val lines = KafkaUtils.createStream(jsc, "Master:2181,Worker1:2181", "StreamingWordCountSelfKafkaScala", topic.toMap).map(_._2)
  //  KafkaUtils.createStream(jsc, "Master:2181,Worker1:2181", "MyFirstGroup", topic).map(_.2)
    val words = lines.flatMap{_.split(" ")}
    val pairs = words.map(word=>(word,1))
    val wordsCount = pairs.reduceByKey(_+_)
 */
