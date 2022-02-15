package project5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String studentsFile = "src/main/java/project5/resources/students.csv";

        Dataset<Row> studentDf = spark.read()
                .format("csv")
                .option("unferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(studentsFile);

        String gradeChartFile = "src/main/java/project5/resources/grade_chart.csv";

        Dataset<Row> gradeDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(gradeChartFile);

        Dataset<Row> filteredDf = studentDf.join(gradeDf, studentDf.col("GPA").equalTo(gradeDf.col("GPA")))
//                .filter(gradeDf.col("GPA").between(2.0, 3.5)) // the same as "where"
                .where(gradeDf.col("GPA").gt(3.0).and(gradeDf.col("GPA").lt(4.5))// gt->greater than/ lt->lower than
                        .or(gradeDf.col("GPA").equalTo(1.0)))
                .select("student_name",
                        "favorite_book_title",
                        "letter_grade") ;


    }
}
