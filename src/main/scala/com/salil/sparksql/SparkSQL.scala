package com.salil.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by salilsurendran on 8/20/16.
  */
object SparkSQL {

    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Simple Application")
      conf.setMaster("local[1]")

      val sc = new SparkContext(conf)
      val sqlc = new SQLContext(sc)
      val logFile = "people.json"
      val inputDF = sqlc.read.json(logFile)

      val query = inputDF.select(inputDF("age"), inputDF("department"))
        .filter(inputDF("age") < 31)
        .filter(inputDF("age") < 29)
        .groupBy("department")
        .count()

      println("Rows = " + query.collect().length)
    }
}
