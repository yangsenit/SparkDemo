package com.ys.mianshi

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/12.
  */
object CombineByKeyDemo {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("CombineByKeyDemo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val list=List((1,2),(1,3),(2,4),(2,5))
    val rdd1=sc.parallelize(list)
    rdd1.combineByKey(
      createCombiner = (x:Int)=>(x:Int,1),  //对原来的value改造为新的value
      mergeValue = (c:(Int,Int),v:Int)=>(c._1+v,c._2+v), //新value 和 旧value的整合
    mergeCombiners = (c1:(Int,Int),c2:(Int,Int))=>(c1._1+c2._1,c1._2+c2._2), //整合后value作为 进行相当于reduceByKey的操作
    2).collect().foreach(println _)
  }
}

/*
结果
(2,(9,2))
(1,(5,2))
 */
