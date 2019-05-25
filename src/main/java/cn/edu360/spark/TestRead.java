package cn.edu360.spark;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;

public class TestRead {

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
//        Dataset<Row> sql = spark.sql("select * from global_temp.testCacheAndRead");
//        sql.show();
        Dataset<Row> csv = spark.read().csv("file:///D:\\key\\test.txt");
        csv.createOrReplaceGlobalTempView("testCacheAndRead");
        System.out.println("成功");

        spark.read().table("spark_write_mysql_to_hive_test").show();

//        SparkSession sparkRead = SparkSession
//                .builder()
//                .appName("testCacheAndRead")
//                .master("local[*]")
//                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
//                .config("hadoop.home.dir", "/user/hive/warehouse")
//                .enableHiveSupport()
//                .getOrCreate();
//        Dataset<Row> sql = sparkRead.sql("select * from global_temp.testCacheAndRead");
//        sql.show();
//        Dataset<Row> sql2 = sparkRead.sql("drop table if exists global_temp.testCacheAndRead");
//
//        SparkSession sparkThrid = SparkSession
//                .builder()
//                .appName("testCacheAndRead")
//                .master("local[*]")
//                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
//                .config("hadoop.home.dir", "/user/hive/warehouse")
//                .enableHiveSupport()
//                .getOrCreate();
//        Dataset<Row> sql1 = sparkThrid.sql("select * from global_temp.testCacheAndRead");
//        sql1.show();

    }
}