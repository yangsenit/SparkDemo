package day0809

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ys on 16-8-11.
  */

object Kmeans_Test {
  def main(args: Array[String]) {
    val start=System.currentTimeMillis()
    val conf=new SparkConf().setAppName("Kmeans_Test").setMaster("spark://master1:7077")
    //val conf=new SparkConf().setAppName("Kmeans_Test").setMaster("local[*]")
    val sc=new SparkContext(conf)
    var path=""
    if(!args.isEmpty){
      path=args(0)
    }else{
      path="file:///home/ys/IdeaProjects/SparkDemo/data/kmeas_test.txt"
    }
    val data=sc.textFile(path)
    val parsedData=data
      .map(x=>{
       Vectors.dense( x.trim.split(",").map(_.trim.toDouble))   //注意RDD的map和数组的map
      })
      .cache()
    val numClusters=3
    val numIterations=20
    //val runs=10
    //获得分类器
    val clusters=new KMeans().setMaxIterations(20).setK(numClusters).run(parsedData)
    //println(parsedData.map(v=>v.toString()+"belong to cluster"+clusters.predict(v)).collect().mkString("\n"))
    //损失
    val WSSSE=clusters.computeCost(parsedData)
    println("WithinSet Sum of Squared Error = "+WSSSE)
//    val a21=clusters.predict(Vectors.dense(1,4,5))
//    val a22=clusters.predict(Vectors.dense(2,5,3))
    //打印出中心点
    println("Clustercenters")
    for(center <- clusters.clusterCenters){
      println(""+center)
    }
//    println("Prediction of (1,4,5)-->"+a21)
//    println("Prediction of (2,5,3)-->"+a22)
    val end=System.currentTimeMillis()
    val consumeTime=end-start
    println("三千万条数据需要的时间 ： "+ consumeTime)
  }
}
