package com.ys.mianshi

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by Administrator on 2016/8/11.
  */
/*
  id1,user1,2,http://www.hupu.com
	id1,user1,2,http://www.hupu.com
	id1,user1,3,http://www.hupu.com
	id1,user1,100,http://www.hupu.com
	id2,user2,2,http://www.hupu.com
	id2,user2,1,http://www.hupu.com
	id2,user2,50,http://www.hupu.com
	id2,user2,2,http://touzhu.hupu.com

  这是我们最后想要的结论
	id1,user1,4,1
	id2,user2,4,2

  这是我们最后抽象出来的 需要完成的任务
  id	user	浏览网页的数量	浏览了多少种网页

 */
object text1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("text1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///workstation/sparkdemo/SparkDemo/data/mianshi_text1.txt")
    //rdd1.collect().foreach( print _)
    val rdd2 = rdd1.map(x => {
      val arr = x.trim.split(",")
      (arr(0) + " " + arr(1), 1)
    })
      .reduceByKey((x, y) => x + y)

    val rdd3 = rdd1.map(x=>{
      val arr=x.trim.split(",")
      (arr(0)+" "+arr(1)+" "+arr(3),1)
    })
    .reduceByKey((x,y)=>x+y)
    //rdd3.collect().foreach( println _)
    .map(x=>{
      val arr=x._1.trim.split(" ")
      (arr(0)+" "+arr(1),x._2)
    })

    val rdd4=rdd2.join(rdd3)
    rdd4.foreach(println _)
  }
}
