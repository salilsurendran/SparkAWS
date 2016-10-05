package com.salil.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by salilsurendran on 10/4/16.
  */
object MassiveSQL {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rdd = sc.textFile("/user/root/people.csv")
    val schemaString = "first_name last_name code"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    import org.apache.spark.sql.Row
    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType, StructField, StringType}
    val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = hiveContext.sql("select code,description,salary as sal from sample_07")
    val df2 = df.join(peopleDataFrame,df.col("code").equalTo(peopleDataFrame("code")))
    println("toString : " + df2.toString())
    println("explain Spark SQL: " )
    df2.explain(true)
  }
}
