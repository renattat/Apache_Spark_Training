package project4;

import mapper.HouseMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pojos.House;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;

public class CsvDatasetHouseToDataframe {

    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<House> and back")
                .master("local")
                .getOrCreate();

        String filename = "src/main/java/project4/resources/houses.csv";

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .option("sep", ";")
                .load(filename);
        System.out.println("House ingested in a dataframe");
//        df.show(5);
//        df.printSchema();

        Dataset<House> houseDs = df.map(new HouseMapper(), Encoders.bean(House.class));

        System.out.println("**** House ingested in a dataset ****");
//        houseDs.show(5);
//        houseDs.printSchema();

        Dataset<Row> df2 = houseDs.toDF();
        df2 = df2.withColumn("formatedDate", concat(df2.col("vacantBy.date"), lit("_"), df2 .col("vacantBy.year")));
        df2.show(10);
    }
}
