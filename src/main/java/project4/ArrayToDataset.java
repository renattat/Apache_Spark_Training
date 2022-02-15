package project4;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {


    public void start() {
        SparkSession spark = new SparkSession.Builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[]{"Banana", "Car", "Glass", "Banana", "Computer", "Car"};

        List<String> data = Arrays.asList(stringList);

        // ds -> dataset
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());

        ds.show(10);

        String stringValue = ds.reduce(new StringReducer());

        System.out.println(stringValue);


//        Dataset<Row> df = ds.toDF();
//        ds = df.as(Encoders.STRING());
//        ds.printSchema();
//        ds.show();
//
//        Dataset<Row> df2 = ds.groupBy("value").count();
//        df2.show();
    }

    static class StringReducer implements ReduceFunction<String>, Serializable {

        @Override
        public String call(String v1, String v2) throws Exception {
            return  v1+v2;
        }
    }
}
