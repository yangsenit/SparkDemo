package com.ys.day0715

import org.apache.commons.net.io.SocketInputStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ys on 16-7-15.
  */
object RDDDemo {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("RDDDemo").setMaster("local[*]")
    val ssc=new StreamingContext(conf,Seconds(10))
    val message= ssc.socketTextStream("localhost", 9999)
    message.transform(eachRDD=>{
      eachRDD.map(x=>(x,1)).reduceByKey(_+_)

    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
