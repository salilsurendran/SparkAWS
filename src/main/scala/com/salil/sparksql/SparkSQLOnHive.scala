package com.salil.sparksql

import org.apache.spark.SparkContext

/**
  * Created by salilsurendran on 8/16/16.
  */

object SparkSQLOnHive {
  def main(args: Array[String]) {
    val sqlContext = new org.apache.spark.sql.SQLContext(new SparkContext())
    if (sqlContext.sql("SHOW TABLES").collect().length == 0) {
      sqlContext.sql("CREATE TABLE sample_07 (code string,description string,total_emp int,salary int)")
      sqlContext.sql("LOAD DATA INPATH '/user/root/sample_07.csv' OVERWRITE INTO TABLE sample_07")
    }
    var df = sqlContext.sql(args(0))
    df = df.filter(df("sal") > 180000)
    df.write.saveAsTable("sample_07_150k_" + System.currentTimeMillis())
    df.write.parquet("/user/root/sample_07_150k_pq_" + System.currentTimeMillis())
    df.write.json("/user/root/sample_07_150k_json_" + System.currentTimeMillis())
    //df.write.text("/user/root/sample_07_150k_text_" + System.currentTimeMillis())
    println("toString : " + df.toString())
    println("explain Spark SQL: " )
    df.explain(true)
    for(i <- 0 until df.inputFiles.length){
      println("i'th element is: " + df.inputFiles(i));
    }
  }
}
