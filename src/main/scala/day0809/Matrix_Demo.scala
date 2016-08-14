package day0809

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ys on 16-8-9.
  */
object Matrix_Demo {
  def main(args: Array[String]) {
    val conf =new SparkConf().setAppName("Matrix_Demo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val dataRdd=sc.textFile("file:///home/ys/data/data1.txt")
    val rdd1=dataRdd
      .map(line=>line.trim.split(","))
      .map(arr=>{
        //当这个矩阵不是第零行，也不是最后一行，不是第一列，也不是最后一列
        /*
        0	0	0	0	0	0	0	0	0	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	1	2	3	4	5	6	7	8	0
        0	0	0	0	0	0	0	0	0	0
         */
      if(arr(0).toInt!=0&&arr(0).toInt!=9&&arr(1).toInt!=0&&arr(1).toInt!=9){
        val i=arr(0).toInt
        val j=arr(1).toInt
        val a=arr(2).toDouble
        List(
          ((i-1,j-1),a/8),
          ((i-1,j),a/8),
          ((i-1,j+1),a/8),
          ((i,j-1),a/8),
          ((i,j+1),a/8),
          ((i+1,j-1),a/8),
          ((i+1,j),a/8),
          ((i+1,j+1),a/8)
        )
      }else{
        List()
      }
    })
      .flatMap(x=>x)
      .reduceByKey((x,y)=>x+y)
    rdd1.repartition(1).saveAsTextFile("file:///home/ys/data/result1")
  }
}
