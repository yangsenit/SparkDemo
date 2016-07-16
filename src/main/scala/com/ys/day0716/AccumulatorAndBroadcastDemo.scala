package com.ys.day0716

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ys on 16-7-16.
  */
object AccumulatorAndBroadcastDemo {
  def main(args: Array[String]) {
    val conf =new SparkConf().setMaster("local[*]").setAppName("AccumulatorAndBroadcastDemo")
    val ssc=new StreamingContext(conf,Seconds(10))
    //broadcast 接受任何类型的数据作为 broadcast的值 value:T
    val broadlist=ssc.sparkContext.broadcast(Array("hadoop","mahout"))
    val ac=ssc.sparkContext.accumulator(0,"blacknamenum")
    val lines=ssc.socketTextStream("localhost",9999)
    //以上所有的动作都是在driver中执行的
    lines.foreachRDD(
      //思考一个问题  rdd是在executor中执行的，
      rdd=>{
      rdd.map(x=>x.trim).filter(x=>{
        if (!broadlist.value.contains(x)){
          //ac.setValue(ac.add(1) )
          ac.add(1)
          /*
          * ERROR org.apache.spark.executor.Executor - Exception in task 1.0 in stage 1.0 (TID 2)
            java.lang.UnsupportedOperationException: Can't read accumulator value in task
          * */
          //println(ac.value)  错误，task中 accumulator 是不可以读的 但是可写（就是修改），所有println(ac.value)报错
          true
        }else{
          false
        }
      }).collect()
        //到这一步，一个完整的RDD计算完毕，并且将这个Rdd中的数据 收集到collect（）中，传回给driver
      println("当前黑名单数量为： "+ac.value)//在driver中可以读取修改 ac的值
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
