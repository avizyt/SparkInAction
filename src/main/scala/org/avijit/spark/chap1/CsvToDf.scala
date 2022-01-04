package org.avijit.spark.chap1

import org.apache.spark.sql.SparkSession

object CsvToDf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("csv")
      .option("header", "true")
      .load("data/books.csv")

    df.show(5)

    spark.stop()

  }
}
