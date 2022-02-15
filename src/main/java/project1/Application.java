package project1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {
    public static void main(String[] args) {

        // Create a swssion
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // get data
        // df = dataFrame
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/java/project1/resources/name_and_comments.txt");

//        df.show(3);


        // Transformation
        df  = df.withColumn("fullName",
                concat(df.col("last_name"), lit(", "), df.col("first_name")))
                .filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc());

        String dbConnectionUrl = "jdbc:postgresql://127.0.0.1:5432/ApacheSparkCourse";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user","postgres");
        prop.setProperty("password", "password");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);

    }
}
