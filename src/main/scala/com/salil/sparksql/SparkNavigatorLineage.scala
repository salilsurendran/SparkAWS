package com.salil.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by salilsurendran on 9/30/16.
  */
object SparkNavigatorLineage {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Lineage Application"))
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    val dfFromHive = hiveContext.sql("from sample_07 select code,description,salary")
    dfFromHive.select("code", "description").write.saveAsTable("new_sample_07_" + System.currentTimeMillis())

    val dfCustomers = sqlContext.read.load("/user/root/customers.parquet").select("id","name")
    dfCustomers.write.save("/user/root/abc_" + System.currentTimeMillis() + ".parquet")

    val rdd = sc.textFile("/user/root/people.csv")
    val outputRDD = rdd.map(_.split(",")).filter(p => p(1).length > 8).map(x => x(0) + ":" + x(1))
    outputRDD.saveAsTextFile("/user/root/output_" + System.currentTimeMillis())
  }
}
