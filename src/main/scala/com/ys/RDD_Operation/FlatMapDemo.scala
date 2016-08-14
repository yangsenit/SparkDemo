package com.ys.RDD_Operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ys on 16-8-14.
  */
object FlatMapDemo {
  def main(args: Array[String]) {
    val path="/home/ys/IdeaProjects/SparkDemo/data/1.txt";
    val conf=new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile(path)

    /*
      def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
      val cleanF = sc.clean(f)
      new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
    }

    type TraversableOnce[+A] = scala.collection.TraversableOnce[A]

    flatMap，函数的定义的，参数是一个  function 这个function的参数是任意类型， 但是参数函数的返回要是一个集合的类型

     */


    //符合要求，匿名函数 的参数  是一个String类型，返回值是一个集合类型，其实是非常好理解的 呵呵
    rdd1.flatMap(line=>line.split(","))  //非常正确  ，其实确实很多问题都是在源码，上都可以得到答案
      .collect().foreach(println _ )

  }
}
