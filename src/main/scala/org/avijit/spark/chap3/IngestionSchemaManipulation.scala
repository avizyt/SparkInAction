package org.avijit.spark.chap3

import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession

object IngestionSchemaManipulation extends App {

  val spark = SparkSession.builder()
    .appName("Restaurant in Wake county, NC")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  var df = spark.read
    .format("csv")
    .option("header", "true")
    .load("data/Restaurants_in_Wake_County.csv")

  df.show(5)
  df.printSchema()
  println("We have "+ df.count() +" number of record.")

  // transforming dataframe
  // transforming dataframe
  df = df.withColumn("county", lit("Wake"))
    .withColumnRenamed("HSISID", "datasetId")
    .withColumnRenamed("NAME", "name")
    .withColumnRenamed("ADDRESS1", "address1")
    .withColumnRenamed("ADDRESS2", "address2")
    .withColumnRenamed("CITY", "city")
    .withColumnRenamed("STATE", "state")
    .withColumnRenamed("POSTALCODE", "zip")
    .withColumnRenamed("PHONENUMBER", "tel")
    .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
    .withColumnRenamed("FACILITYTYPE", "type")
    .withColumnRenamed("X", "geoX")
    .withColumnRenamed("Y", "geoY")
    .drop("OBJECTID")
    .drop("PERMITID")
    .drop("GEOCODESTATUS")

  df = df.withColumn("id",
    concat(
    df.col("state"), lit("-"),
    df.col("county"), lit("-"),
    df.col("datasetID")))

  println("*** Dataframe transformed")
  df.show(5)

}
