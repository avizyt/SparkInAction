package org.avijit.spark.chap3

import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.Partition

import org.apache.spark.sql.SparkSession


object IngestionSchemaManipulationApp extends App{

  val spark = SparkSession.builder()
    .appName("Restaurant in Wake county, NC")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var df = spark.read
    .format("csv")
    .option("header", "true")
    .load("data/Restaurants_in_Wake_County.csv")

  println("*** Right after ingestion")

  df.show(5)
  df.printSchema()
  println("We have " + df.count() + " records.")


}
