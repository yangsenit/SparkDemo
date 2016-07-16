import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ys on 16-7-16.
  */
object text1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("text1").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val hiveContext=new HiveContext(sc)
    val rdd1=sc.textFile("G:\\workstation\\SparkDemo\\data\\1.txt").map(x=>{x.trim.split(",")})
    rdd1.foreach(print _)
  }
}
