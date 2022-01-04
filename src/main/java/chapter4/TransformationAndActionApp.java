package chapter4;

import  static org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransformationAndActionApp {
    public static void main(String[] args) {
        TransformationAndActionApp app = new TransformationAndActionApp();
        String mode = "noop";
        if (args.length != 0){
            mode = args[0];
        }
        app.start(mode);
    }

    private void start(String mode) {
        long t0 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behaviour")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        long t1 = System.currentTimeMillis();
        System.out.println("1. Creating a session ............." + (t1 - t0));

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/nchs_teenBirthRate.csv");

        Dataset<Row> initialDf = df;
        long t2 = System.currentTimeMillis();
        System.out.println("2: Loading initial dataset.........." + (t2 - t1));

        for (int i = 0; i < 60; i++) {
            df = df.union(initialDf);
        }
        long t3 = System.currentTimeMillis();
        System.out.println("3: Builder full dataset ..............." + (t3 - t2));

        df = df.withColumnRenamed("Lower Confidence Limit","lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        long t4 = System.currentTimeMillis();
        System.out.println("4: Clean-up ..........................."+(t4 - t3));

        if (mode.compareToIgnoreCase("noop") != 0){
            df = df.withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0){
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        long t5 = System.currentTimeMillis();
        System.out.println("5: Transformation ............."+ ( t5 - t4));

        df.collect();
        long t6 = System.currentTimeMillis();
        System.out.println("6: Final action ................."+(t6 - t5));

        System.out.println(" ");
        System.out.println("# of records .................." + df.count());

    }
}
