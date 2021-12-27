package org.avijit.spark.chap2

import java.util.Properties

import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToDataBaseScalaApp {
  def main(args: Array[String]): Unit = {
    // create session on local master
    val spark = SparkSession.builder()
      .appName("CSV to DB")
      .master("local[*]")
      .getOrCreate()

    // step 1: Ingestion
    // read the csv file with header and store it in a Dataframe
    var df = spark.read
      .format("csv")
      .option("header", "true")
      .load("data/authors.csv")

    // step 2: transform
    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column
    df = df.withColumn("name", concat(col("lname"), lit(", "), col("fname")))

    // step 3: save
    // ----
    // The connection URL, assuming your PostgreSQL instance runs locally on
    // the
    // default port, and the database we use is "spark_labs"
    val dbConnectionUrl = "jdbc:mysql://localhost:3306/spark_lab"

    // Properties to connect to the database, the JDBC driver is part of our
    // pom.xml
    val prop = new Properties
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    prop.setProperty("user", "Avijit")
    prop.setProperty("password", "mysqlavijit1234")

    // Write in a table called ch02
    df.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch2", prop)


    // Good to stop SparkSession at the end of the application
    spark.stop

    println("Process complete")
  }
}
