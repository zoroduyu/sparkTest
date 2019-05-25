package cn.edu360.spark;


import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.ParseException;
import java.util.Properties;

public class TestCache {

    private static final String mysqlUser = "root";
    private static final String mysqlPWD = "root";
    private static final String jdbcUrl = "jdbc:mysql://192.168.10.200:3306/testdb";
    private static final String jdbcDriver = "com.mysql.jdbc.Driver";

    public static void main(String[] args) throws ParseException {
        SparkSession spark = SparkSession
                .builder()
                .appName("testCacheAndRead")
                .master("local[*]")
              //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> csv = spark.read().csv("file:///D:\\key\\test.txt");
        csv.createOrReplaceGlobalTempView("testCacheAndRead");
        System.out.print("成功");
    }
}