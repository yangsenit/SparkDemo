package com.ys.RDD_Operation

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/6.
  */
object TakeDemo {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("TakeDemo").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val RDD1=sc.parallelize(List(1,2,3,4,5,6,7,8))
      RDD1.take(3).foreach(print _)
    }
}
