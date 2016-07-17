/*
  累加器的使用
 */

package com.ys.day0716

import java.sql.PreparedStatement

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ys on 16-7-16.
  */
object StreamingInsertToMysql {
  def main(args: Array[String]) {
    val conf =new SparkConf().setMaster("local[*]").setAppName("AccumulatorAndBroadcastDemo")
    val ssc=new StreamingContext(conf,Seconds(10))
    //broadcast 接受任何类型的数据作为 broadcast的值 value:T
    val broadlist=ssc.sparkContext.broadcast(Array("hadoop","mahout"))
    val ac=ssc.sparkContext.accumulator(0,"blacknamenum")
    val lines=ssc.socketTextStream("localhost",9999)
    lines.foreachRDD(rdd=>{
      rdd.foreachPartition(part=>{
        //对于每个partition我们创建一个连接
        val conn=new Connec2Mysql().connec
        //关于insert 的语句只说一句话，就是 insert into AccumulatorAndBroadcastDemo (notblackname) values ("123");
        //属性和添加的值 全部用括号给括起来
        //var sql="insert into AccumulatorAndBroadcastDemo （notblackname） values （?）"
        part.map(x=>x.trim).filter(x=>{
          if (!broadlist.value.contains(x)){
            //insert into AccumulatorAndBroadcastDemo （notblackname） values （’ 12’）
            val sql: String = "insert into AccumulatorAndBroadcastDemo (notblackname) values (?)"
            val state: PreparedStatement = conn.prepareStatement(sql)
            state.setString(1, x)
            //Statement state= (Statement) conn.createStatement();
            state.executeUpdate()
            state.close()
            true
          }else{
            false
          }
        }).foreach(x=>x)
        conn.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

