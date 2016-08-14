package day0809
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
/**
  * Created by ys on 16-8-9.
  * A B C D E F
  * B A C D E
  * C A B E
  * D A B E
  * E A B C D
  * F A c
  */
object Find_Friend {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("Find_Friend").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("file:///home/ys/IdeaProjects/SparkDemo/data/friends.txt")
    val rdd2=rdd1
      .map(line=>{
      val arr=line.trim.split(" ")//切分后为 String类型的数组
      val len=arr.length
      val listBuf=new ListBuffer[(String,String)]
        for(i <-1 to  arr.length-1-1){
          for(j<-i+1 to arr.length-1){
            val temp=arr(i)+arr(j)
          listBuf+=((temp,arr(0)))
        }
      }
      listBuf.toList
    })
    rdd2.collect
  }
}
