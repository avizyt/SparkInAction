package chapter3;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationApp {
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();

    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake Country, NC")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County.csv");
        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");
    }
}
