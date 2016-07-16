//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.Seconds
//
///**
//  * Created by ys on 16-7-16.
//  */
//object text1 {
//  def main(args: Array[String]) {
//    val conf =new SparkConf().setAppName("text1").setMaster("local[*]")
//    val sc=new SparkContext(conf)
//    case class bk(x:String,y:String,z:String)
//    val sqlContext=new SQLContext(sc)
//    import sqlContext.implicits._
//    sc.textFile("/home/ys/IdeaProjects/SparkDemo/data/1.txt").map(x=>x.trim.split(",")).map(x=>bk(x(0),x(1),x(2))).toDF().registerTempTable("temp")
//    sqlContext.sql("select * from temp").show()
//
//  }
//}
