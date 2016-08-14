package com.ys.mianshi

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/11.
  */
object AggregateByKeyDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateByKetDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list=List((1,2),(1,3),(2,2),(2,4))
    val rdd1=sc.parallelize(list)

    def seq(x:Int,y:Int):Int={  //传入的参数都是  value ，目的是重构 value
      Math.max(x,y)
    }
    def comb(x:Int,y:Int):Int={ // 传入的是两个相同的key的两个不同的value，相当于reduceByKey中value合并策略
      x+y
    }
    rdd1.aggregateByKey(4,2)(seq,comb).collect().foreach(println _)
  }
}
/*
  结果
  (1,8)
  (2,8)
 */
/*
  说说AggregateByKey是怎样工作的
  （4,2）
   4是最开始给的值
   2是分区数
   seq是怎样选择value 比如 （1,2）的时候 2<4 的所以就变成(1,4) 同样（1,3）变成（1,4） （2,2）变成（2,4） ...
   seq 其实可以理解为怎样 对原有的数据key不变 value进行重新修改
   comb 相当reduceByKey中 value的合并的策略
 */