package chapter3;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationJavaApp {
    public static void main(String[] args) {
        IngestionSchemaManipulationJavaApp app = new IngestionSchemaManipulationJavaApp();
        app.start();
    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurant in Wake county, NC")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County.csv");

        System.out.println("=== Right after ingestion ===");
        df.show(5);
        df.printSchema();
        System.out.println("We have "+ df.count()+ " number of record.");

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
                .drop("GEOCODESTATUS");

        df = df.withColumn("id", concat(
                df.col("state"), lit("-"),
                df.col("county"), lit("-"),
                df.col("datasetID")
        ));

        System.out.println("*** DataFrame transformed");
        df.show(5);

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);
        System.out.println("partition count after repartition: "+
                df.rdd().partitions().length);



    }
}
