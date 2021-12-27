package org.avijit.spark.examples

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

object RDDeg1 extends App {
    // create Dataframe using SparkSession
  val spark = SparkSession.builder()
    .appName("RDD Example")
    .master("local[*]")
    .getOrCreate()

  //create a Dataframe of names and ages
  val df = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
    ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

  // group the same names together, agg their age, compute an avg
  val avgDF = df.groupBy("name").agg(avg("age"))

  avgDF.show(5)
  spark.stop()

}
