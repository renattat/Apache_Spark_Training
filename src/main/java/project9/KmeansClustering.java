package project9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KmeansClustering {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = new SparkSession.Builder()
                .appName("kmeansClustering")
                .master("local")
                .getOrCreate();

        Dataset<Row> wholeSaleDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("src/main/java/project9/resourcecs/Wholesale+customers+data.csv");

        Dataset<Row> featuresDf = wholeSaleDf.select("Channel","Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen");

        VectorAssembler assembler = new VectorAssembler();

        assembler = assembler.setInputCols(new String[]{"Channel","Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen"})
                .setOutputCol("features");

        Dataset<Row> trainingData = assembler.transform(featuresDf).select("features");

        KMeans kMeans = new KMeans().setK(10);

        KMeansModel model = kMeans.fit(trainingData);


//        System.out.println(model.computeCost(trainingData));
        model.summary().predictions().show();

    }
}
