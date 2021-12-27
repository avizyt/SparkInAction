package chapter2;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CsvToRdbms {
    public static void main(String[] args) {
        CsvToRdbms app = new CsvToRdbms();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        df = df.withColumn(
                "name",
                concat(df.col("lname"),
                        lit(", "), df.col("fname"))
        );

        String dbConnectionUrl = "jdbc:mysql://localhost:3306/spark_lab";
        Properties prop = new Properties();
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        prop.setProperty("user", "Avijit");
        prop.setProperty("password", "mysqlavijit1234");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch2", prop);
        System.out.println("Process complete");

    }
}
