package com.salil.sparksql

import org.apache.spark.SparkContext


/**
  * Created by salilsurendran on 10/5/16.
  */
object SparkTempTableExample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    case class Person(name: String, age: Long)
    val peopleDF = sc.textFile("/user/root/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    // Register the DataFrame as a temporary view
    peopleDF.registerTempTable("foo2thebar")

    val df = sqlContext.sql("SELECT name, age FROM foo2thebar WHERE age BETWEEN 13 AND 19")
    df.write.json("/user/root/people_json_" + System.currentTimeMillis())
    println("toString : " + df.toString())
    println("explain Spark SQL: " )
    df.explain(true)
  }
}
