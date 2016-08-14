package com.ys.RDD_Operation

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/6.
  */
object CartesianDemo {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("CartesianDemo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val RDD1=sc.parallelize(List(1,2,3,4,5,6,7),2)
    val RDD2=sc.parallelize(List(1,1,1,1,1,1,1))
    val RDD3=RDD1.cartesian(RDD2)
    def gk(x:Int):Int={
      if(x%2==0)
        1
      else
        2
    }
    /*
    两个知识点
      1.一个函数需要用函数作为参数的时候怎么传参
      2.groupBy（）需要自己去定义K  用自定的函数去生成K
      3.groupBy（）是对一个RDD 进行处理的，是对一个RDD的参数中不同的分区进行处理的
     */
    val RDD4=RDD1.groupBy(gk _,2)  //(2,CompactBuffer(1, 3, 5, 7))(1,CompactBuffer(2, 4, 6))
    RDD3.foreach(x=>print(x))
    print("***************************************************")
    RDD4.foreach(x=>print(x))
  }
}
