package org.avijit.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SchemaDataFrame extends App {

    val spark = SparkSession.builder()
      .appName("Schema Dataframe")
      .master("local[*]")
      .getOrCreate()

//    if (args.length <= 0){
//      println("usage SchemaDataframe < file path to blog.csv>")
//      System.exit(1)
//    }

    val customSchema = StructType(Array(StructField("ID", IntegerType, false),
      StructField("First", StringType,false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)
    ))

    val df = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load("data/blog.csv")


    df.show()
//    println(df.printSchema)
//    println(df.schema)

    spark.stop()




}
