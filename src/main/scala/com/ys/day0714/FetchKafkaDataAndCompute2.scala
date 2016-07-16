//package com.ys.day0714
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//
///**
//  * Created by ys on 16-7-14.
//  */
//object FetchKafkaDataAndCompute2 {
//  def main(args: Array[String]) {
//    // args 是个数组，数组中的每一个元素都是 String
//    val conf = new SparkConf().setAppName("FetchKafkaDataAndCompute").setMaster("local[*]")
//    val ssc = new StreamingContext(conf, Seconds(10))
//    ssc.checkpoint("/home/ys/checkpoint/kafka")
////    val kafkaParameters=Map("metadata.broker.list"->"master1:9092,slave1:9092,slave2:9092")
////    val topic =Set[String]("my-topic")
//    //只想说一点的是  这里的泛型必须加上 ，必须加上，要不根本就没有法子去解析，kafka中的数据是什么类型，给了一StringDecoder的方式去 解析获得key和value
//    //[String,String,StringDecoder, StringDecoder] 告诉spark这样解析 进来的数据是key 是String类型 value也是String类型
//    // 解析的时候 key用String的方式解析 value用String的方式解析
//    //泛型的一定不能掉
////    val lines= KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParameters,topic).map(_._2)
//    /*
//    2016-07-15	1468568388149	17446	9	kafka	view
//    2016-07-15	1468568388650	91543	12	HBase	register
//    2016-07-15	1468568389151	81086	18	hadoop	view
//    2016-07-15	1468568389652	85759	11	spark	view
//    2016-07-15	1468568390154	62068	0	ML	view
//    2016-07-15	1468568390655	56396	14	impala	view
//    private var data: String = null
//    private var timeStamp: String = null
//    private var userID: String = null
//    private var pageID: String = null
//    private var channelID: String = null
//    private var action: String = null
//    private var userLog: String = null
//     */
//    //这样的数据类型
//    //1，获得数据
//    //2，我们想用sparkSql 操作
//    //搞定数据结构
////    val schema=
////      StructType(
////            StructField("date",StringType,false) ::
////            StructField("timeStamp", StringType, false) ::
////            StructField("userID", StringType, false) ::
////            StructField("pageID", StringType, false) ::
////            StructField("channelID", StringType, false) ::
////            StructField("action", StringType, false) ::Nil)
////    //准备数据
//
//    //使用 case class  隐式转换的方式  构造 df 在使用sql的方式操作的
//    val kafkaParameters=Map("metadata.broker.list"->"master1:9092,slave1:9092,slave2:9092")
//    val topic =Set[String]("my-topic")
//    val lines= KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParameters,topic).map(_._2).map(_.split("\t"))
//    case class pp(data: String,timeStamp: String,userID: String,pageID: String,channelID: String,action: String)
//    val sqlContext=new SQLContext(ssc.sparkContext)
//    lines.foreachRDD(rdd=> {
//      import sqlContext.implicits._
//      //rdd.map(x=>{pp(x(0),x(1),x(2),x(3),x(4),x(5))}
//
//      ).toDF().registerTempTable("temp")
//      sqlContext.sql("select count(*) from temp").show()
//    })
//      //    val line1=lines.map(_.split("\t"))
//      //    val result=line1.transform(rdds=>{
//      //      val row=rdds.map(arr=>{Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5))})
//      //      val df=sqlContext.createDataFrame(row,schema)
//      //      df.registerTempTable("TempDf")
//      //      sqlContext.sql("select * from TempDf where action='view'").rdd
//      //    }).print()
//
//      ssc.start()
//      ssc.awaitTermination()
//  }
//}
//
//    /*
//
//    sqlContext.sql("select * from TempDf where action='view'").rdd
//    这句话也道出了一个方法 DataFrame到RDD  只需要 df.rdd 就可以了
//    DStream to RDD by transform function
//    RDD => RDD[Row]
//    Rdd[Row] to DataFrame
//    DataFrame =>df.rdd
//    logical task we can do another but the frame has been built
//
//16:38:10.092 [streaming-job-executor-0] INFO  o.a.spark.scheduler.DAGScheduler - Job 6 finished: print at FatchKafkaDataAndCompute.scala:63, took 0.026703 s
//-------------------------------------------
//Time: 1468571890000 ms
//-------------------------------------------
//[2016-07-15,1468571880717,28863,11,impala,view]
//[2016-07-15,1468571881218,49834,17,HBase,view]
//[2016-07-15,1468571881719,26723,4,ML,view]
//[2016-07-15,1468571883222,63134,7,impala,view]
//[2016-07-15,1468571883724,57872,18,impala,view]
//[2016-07-15,1468571884726,59534,1,flume,view]
//[2016-07-15,1468571886730,69632,15,impala,view]
//[2016-07-15,1468571887232,35871,6,impala,view]
//[2016-07-15,1468571887732,97871,18,spark,view]
//[2016-07-15,1468571888734,9354,1,ML,view]
//
//    */
//
///*
////创建Kafka元数据，来让Spark Streaming这个kafka Consumer利用
//    val kafkaParameters= new HashMap[String,String]()
//    kafkaParameters.put("metadata.broker.list","Master:9092,Worker1:9092")
//    val topic = new HashSet[String]()
//    topic.add("HelloKafka")
//
//    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](jsc,kafkaParameters.toMap,topic.toSet).map(_._2)
//  //  val lines = KafkaUtils.createStream(jsc, "Master:2181,Worker1:2181", "StreamingWordCountSelfKafkaScala", topic.toMap).map(_._2)
//  //  KafkaUtils.createStream(jsc, "Master:2181,Worker1:2181", "MyFirstGroup", topic).map(_.2)
//    val words = lines.flatMap{_.split(" ")}
//    val pairs = words.map(word=>(word,1))
//    val wordsCount = pairs.reduceByKey(_+_)
// */
