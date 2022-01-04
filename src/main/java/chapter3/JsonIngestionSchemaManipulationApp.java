package chapter3;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonIngestionSchemaManipulationApp {
    public static void main(String[] args) {
        JsonIngestionSchemaManipulationApp app = new JsonIngestionSchemaManipulationApp();
        app.start();
    }

    public void start(){
        SparkSession spark = SparkSession.builder()
                .appName("Restaurant in Durham County.")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("json")
                .load("data/DurhamRestaurant.json");

        System.out.println("*** Right after ingestion.");
        df.show();
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        df = df.withColumn("id",
                concat(df.col("state"), lit("_"),
                        df.col("county"), lit("_"),
                        df.col("datasetID")));


        System.out.println("*** Dataframe transformed");
        df.show();
        df.printSchema();
    }
}
