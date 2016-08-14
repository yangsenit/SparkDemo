package com.ys.RDD_Operation

//import akka.event.slf4j.Logger
import org.apache.log4j.{Logger, Level}
import org.apache.spark
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/8/7.
  */
object DataSetDemo {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    val conf=new SparkConf().setAppName("CartesianDemo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val ds1 = Seq(1, 2, 3).toDS()
    val ds2=List(1,2,3).toDS()
    val ds3=sc.parallelize(List(1,2,3)).map(x=>(x,x*x)).toDS()
    val ds3ToRdd1=ds3.rdd
    val df1=ds1.toDF()
    val df2=ds2.toDF()

    df1.show()
    df2.show()
    ds3.show()
    ds3ToRdd1.foreach(print _)
  }
}
