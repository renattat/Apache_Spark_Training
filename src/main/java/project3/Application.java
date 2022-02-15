package project3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 DataSets")
                .master("local")
                .getOrCreate();

        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
//        durhamDf.show();
//        durhamDf.printSchema();

        Dataset<Row> philDf = buildPhilParksDataFrame(spark);
//        philDf.show(10);
//        philDf.printSchema();

        combineDataframes(durhamDf, philDf);

        System.out.println(philDf.count());

    }

    private static void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        // Match by column names using the unionByName() method.
        // if we use just the union() method, it matches the columns based on order.
        Dataset<Row> df = df1.unionByName(df2);
        df.show(500);
        df.printSchema();
        System.out.println("we have " + df.count());

        df = df.repartition(5);

        Partition[] partitions = df.rdd().getPartitions();
        System.out.println("Total number of Partitions : " + partitions.length);
    }

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) {
        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("src/main/java/project3/resources/durham-parks.json");

        df = df.withColumn("park_id",
                        concat(df.col("datasetid"),
                                lit("_"),
                                df.col("fields.objectid"),
                                lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acress", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("fields").drop("geometry").drop("record_timestamp").drop("recordid").drop("datasetid");

        return df;
    }

    public static Dataset<Row> buildPhilParksDataFrame(SparkSession spark) {

        Dataset<Row> df = spark.read().format("csv")
                .option("multiline", false)
                .option("header", true)
                .load("src/main/java/project3/resources/philadelphia_recreations.csv");

//        df = df.filter(lower(df.col("USE_")).like("%park%"));
//        or the same:
        df = df.filter("lower(USE_) like '%park%'");

        df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acress")
                .withColumn("geoX", lit("UNKONW"))
                .withColumn("geoY", lit("UNKONW"))
                .drop("SITE_NAME")
                .drop("OBJECTID")
                .drop("CHILD_OF")
                .drop("TYPE")
                .drop("USE_")
                .drop("DESCRIPTION")
                .drop("SQ_FEET")
                .drop("DESCRIPTION")
                .drop("ALLIAS")
                .drop("CHRONOLOGY")
                .drop("NOTES")
                .drop("DATE_EDITED")
                .drop("EDITED_BY")
                .drop("OCCUPANT")
                .drop("TENANT")
                .drop("LABEL");

        return df;

    }
}
