package chapter5;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class DartPiAproximator {
    private static long counter = 0;



    /**
     * Mapper class, creates the map of dots
     */

    private final class DartMapper
            implements MapFunction<Row, Integer>{
        private static final long serialVersionUID = 38446L;

        @Override
        public Integer call(Row r) throws Exception{
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            counter++;
            if (counter % 100000 == 0){
                System.out.println("" + counter + " dart thrown so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }

        /**
         * Reducer class, reduce the map of dots
         */
        private final class DartReducer
                implements ReduceFunction<Integer>{
            private static final long serialVersionUID = 12859L;

            @Override
            public Integer call(Integer x, Integer y){
                return x + y;
            }
        }
        public static void main(String[] args) {
            DartPiAproximator app = new DartPiAproximator();
            app.start(10);

        }

    private void start(int slices){
        int numberOfThrows = 100000 * slices;
        System.out.println("About to throw " + numberOfThrows + " darts to the target.");

        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder()
                .appName("Spark Pi Application.")
                .master("local")
                .getOrCreate();


        spark.sparkContext().setLogLevel("ERROR");

        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1-t0) + "ms");

        // Create a list from the number of throws.
        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            listOfThrows.add(i);
        }

        // convert the list of throws to spark dataframe.
        Dataset<Row> incrementalDF = spark
                .createDataset(listOfThrows,Encoders.INT())
                .toDF();
        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built "+ (t2-t1) + "ms");

        Dataset<Integer> dartsDs = incrementalDF
                .map(new DartMapper(), Encoders.INT());
        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in "+ (t3-t2) + "ms");

        int dartInCircle = dartsDs.reduce(new DartReducer());
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + "ms");

        System.out.println("Pi is roughly " + 4.0 * dartInCircle / numberOfThrows);

        spark.stop();



        }


    }
