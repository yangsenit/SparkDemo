//package com.ys.day0715
//
///**
//  * Created by ys on 16-7-15.
//  */
//// sc is an existing SparkContext.
//object SparkSqlDemo{
//  def main(args: Array[String]) {
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//    // Create an RDD
//    val people = sc.textFile("examples/src/main/resources/people.txt")
//
//    // The schema is encoded in a string
//    val schemaString = "name age"
//
//    // Import Row.
//    import org.apache.spark.sql.Row;
//
//    // Import Spark SQL data types
//    import org.apache.spark.sql.types.{StructType,StructField,StringType};
//
//    // Generate the schema based on the string of schema
//    val schema =
//      StructType(
//        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//
//    // Convert records of the RDD (people) to Rows.
//    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
//
//    // Apply the schema to the RDD.
//    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
//
//    // Register the DataFrames as a table.
//    peopleDataFrame.registerTempTable("people")
//
//    // SQL statements can be run by using the sql methods provided by sqlContext.
//    val results = sqlContext.sql("SELECT name FROM people")
//
//    // The results of SQL queries are DataFrames and support all the normal RDD operations.
//    // The columns of a row in the result can be accessed by field index or by field name.
//    results.map(t => "Name: " + t(0)).collect().foreach(println)
//  }
//}