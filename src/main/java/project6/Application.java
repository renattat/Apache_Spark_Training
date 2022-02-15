package project6;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.desc;

public class Application {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Learning  Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String redditFileSmall = "src/main/java/project6/resources/Reddit_2007-small.json";

        String redditFileLarge = "src/main/java/project6/resources/Reddit_2011-large.json";

        Dataset<Row> redditDf = spark.read()
                .format("json")
                .option("inferSchema", "true")
                .load(redditFileLarge);

        redditDf = redditDf.select("body");

        Dataset<String> wordsDs = redditDf
                .flatMap((FlatMapFunction<Row, String>) row -> Arrays.asList(row.toString().replace("\n","").replace("\r","")
                        .trim().toLowerCase().split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordsDf = wordsDs.toDF();


        Dataset<Row> boringWordsDf = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();
//        wordsDf = wordsDf.except(boringWordsDf);   // -> this removes duplicates from the end dataframe
        wordsDf = wordsDf.join(boringWordsDf, wordsDf.col("value").equalTo(boringWordsDf.col("value")), "leftanti");

        wordsDf = wordsDf.groupBy("value").count();

        wordsDf.orderBy(desc("count")).show(40);

    }
}
