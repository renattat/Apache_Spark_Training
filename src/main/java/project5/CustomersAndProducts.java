package project5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String customers_file = "src/main/java/project5/resources/customers.csv";

        Dataset<Row> customerDf = spark.read()
                .format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(customers_file);

        String products_file = "src/main/java/project5/resources/products.csv";

        Dataset<Row> productDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .load(products_file);

        String purchases_file = "src/main/java/project5/resources/purchases.csv";

        Dataset<Row> purchaseDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .load(purchases_file);

        System.out.println(" Loaded all files into data frames ");
        System.out.println("------------------------------------");

        Dataset<Row> joinedDf = customerDf.join(purchaseDf, purchaseDf.col("customer_id").equalTo(customerDf.col("customer_id")))
                .join(productDf, productDf.col("product_id").equalTo(purchaseDf.col("product_id")))
                .drop("favorite_website")
                .drop(purchaseDf.col("customer_id"))
                .drop(purchaseDf.col("product_id"))
                .drop("product_id");

        Dataset<Row> aggDf = joinedDf.groupBy("first_name", "product_name")
                .agg(count("product_name").as("number_of_purchases"),
                        max("product_price").as("most_exp_purchase"),
                        sum("product_price").as("total_spent"));

        aggDf.show();

    }
}
