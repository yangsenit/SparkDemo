package day0809

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ys on 16-8-10.
  */
object ReduceByKey_Demo {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("ReduceByKey_Demo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(List(("a","b"),("c","d"),("a","d")))
    val rdd2=rdd1.reduceByKey((x,y)=>x+" "+y)
    rdd2.collect
  }
}
