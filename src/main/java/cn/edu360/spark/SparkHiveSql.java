//package cn.edu360.spark;
//
//import java.io.File;
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//
///**
// * Created by ZOROD on 2019/5/22.
// */
//public class SparkHiveSql {
//
//    // warehouseLocation points to the default location for managed databases and tables
//    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
//    SparkSession spark = SparkSession
//            .builder()
//            .appName("Java Spark Hive Example")
//            .config("spark.sql.warehouse.dir", warehouseLocation)
//            .enableHiveSupport()
//            .getOrCreate();
//
//spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
//spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
//
//// Queries are expressed in HiveQL
//spark.sql("SELECT * FROM src").show();
//
//
//
//
//
//}
