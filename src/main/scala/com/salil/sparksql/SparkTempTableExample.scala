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

  case class Person(name: String, age: Long)
}


object TestConf {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val conf = sc.getConf
    println("conf.getAll")
    conf.getAll.foreach(x => println(x._1 +":"+ x._2))
    println("sysEnv foreach")
    sys.env.foreach(println)
    println("executor env getAll")
    conf.getExecutorEnv.foreach(x => println(x._1 +":"+ x._2))
    println("TEst")
    /*val df = spark.read.json("/home/salilsurendran/WORK/lineage/datafiles/root/people.json")
            .select("name","age")
    df.write.json("/home/salilsurendran/WORK/lineage/datafiles/root/people"+ System
            .currentTimeMillis() + ".json")*/
  }
}
