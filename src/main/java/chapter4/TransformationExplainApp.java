package chapter4;

import org.apache.spark.sql.SparkSession;
import  static org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TransformationExplainApp {
    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        app.start();
    }

    public void start(){
        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/nchs_teenBirthRate.csv");
        Dataset<Row> df0 = df;

        df = df.union(df0);

        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        df = df.withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));

        df.explain();
    }
}
