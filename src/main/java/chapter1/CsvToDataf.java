package chapter1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class CsvToDataf {
    public static void main(String[] args) {
        CsvToDataf app = new CsvToDataf();
        app.start();
    }

    private void start(){
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DF")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");

        df.show(5);
    }
}
