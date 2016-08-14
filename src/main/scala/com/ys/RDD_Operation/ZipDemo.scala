package com.ys.RDD_Operation

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/6.
  */
object ZipDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TakeDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD1=sc.parallelize(List(1,2,3,4,5,6,7,8),2)
    //val RDD2=sc.parallelize(List('a','b','c','d','e','f','g','h'),3)
    val RDD2=sc.parallelize(List('a','b','c','d','e','f','g','h'),2)
    val RDD4=sc.parallelize(List(1,2,3,4),2)
    //val RDD3=RDD1.zip(RDD2)
    //RDD3.foreach(print _)
    RDD4.zip(RDD1).foreach( print _)

    /*
    Exception
    in thread "main" java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions
     */
    //使用 zip的操作，两个RDD需要是同样的  partition

    //Can only zip RDDs with same number of elements in each partition
  }
}
