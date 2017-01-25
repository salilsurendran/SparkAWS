package com.salil.sparksql

import java.io.{PrintWriter, File}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by salilsurendran on 10/3/16.
  */
object LineageExample {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Lineage Application"))
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val people = sc.textFile("examples/resources/people.txt")

  // Load a Dataframe where columns are specified in load statement
  val dfSelectFromSrc =
    sqlContext.sql("SELECT a, b, c FROM parquet.`examples/resources/users.parquet`")

  // Load from a folder, using globbing expression
  val globRdd = sc.textFile("s3a://foobar/*.json")

  // Load from a Hive table

  val dfHive = hiveContext.sql("FROM src SELECT h1, h2, h3, h4")

  // Load a Dataframe where columns are selected subsequently
  // What happens here? Can we identify the columns???
  val dfFromJson = sqlContext.read.json(
    "hdfs://examples1/people1.json",
    "hdfs://examples2/people2.json",
    "hdfs://examples3/people3.json")
  val dfFromOperaton = dfFromJson.select("name", "age", "phone", "zip")

  // Load some data just from a File. This will not be tracked for lineage
  /* val fileData = Source.fromFile("/tmp/foo2thebar.txt").getlines.toList

  import org.apache.spark.sql.cassandra._
  val dfFromCass = sqlContext.read.cassandraFormat("words", "test", "cluster_A").load()
    .select("c1", "c4", "c6")*/

  /*
      .... some computation got done here....
   */

  // Write to a single file
  dfSelectFromSrc.write.save("examples/resources/abc.parquet")

  // write rdd to a hdfs folder
  people.saveAsTextFile("s3a://b-datasets/flight_data2/")

  // Show an example of collect() and write out to a file on disk. THIS WILL NOT BE CAPTURED IN LINEAGE!!
  val teenagers =
    hiveContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19").collect()
  val writer = new PrintWriter(new File("examples/resources/Write.txt"))
  teenagers.foreach(writer.println(_))

  // Write a Dataframe to HDFS with partitioning
  dfFromOperaton.write.partitionBy("age", "zip").save("hdfs://user/foobar/partitioned_example")

  // Write data to a Hive Table
  dfHive.select("h1", "h4").write.saveAsTable("output_hive_table")

  // Write to Cassandra. Assume dfCassWrite has columns cw1, cw2, cw3
  /* dfCassWrite.write.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "words_copy", "keyspace" -> "test")).save()*/

}
