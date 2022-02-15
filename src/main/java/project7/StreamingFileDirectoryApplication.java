package project7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.avg;

public class StreamingFileDirectoryApplication {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        System.setProperty("hadoop.home.dir","C:\\hadoop" );

        SparkSession spark = SparkSession.builder()
                .appName("StreamingFileDirectoryWordCount")
                .master("local")
                .getOrCreate();


        // Read all the csv files written automatically in a directory
        StructType userSchema = new StructType().add("date", "string").add("value", "float");

        Dataset<Row> stockData = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema) // specify schema of the csv files
                .csv("src/main/java/project7/resources/incomingStockFiles"); // Equivalent to format("csv").load("src/main/java/project7/resources/incomingStockFiles")

        Dataset<Row> resultDf = stockData.groupBy("date").agg(avg(stockData.col("value")));

        StreamingQuery query = resultDf.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
