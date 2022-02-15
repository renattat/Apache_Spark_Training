package project8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class LinearMarketingVsSales {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = new SparkSession.Builder()
                .appName("LinearProgressionExample")
                .master("local")
                .getOrCreate();

        Dataset<Row> markVsSalesDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("src/main/java/project8/resources/marketing_vs_sales.csv");

        Dataset<Row> mlDf = markVsSalesDf.withColumnRenamed("sales", "label")
                .select("label", "marketing_spend", "bad_day");

        String[] featureColumns = {"marketing_spend", "bad_day"};

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureColumns)
                .setOutputCol("features");

        Dataset<Row> lblFeatures = assembler.transform(mlDf).select("label", "features");
        lblFeatures = lblFeatures.na().drop();

        lblFeatures.show();

        // next we need to create a linear regression model object
        LinearRegression lr = new LinearRegression();
        LinearRegressionModel learningModel = lr.fit(lblFeatures);

        learningModel.summary().predictions().show();

        System.out.println("R squared: " + learningModel.summary().r2());


    }
}
