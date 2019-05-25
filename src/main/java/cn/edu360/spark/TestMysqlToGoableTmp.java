package cn.edu360.spark;


import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.ParseException;
import java.util.Properties;

public class TestMysqlToGoableTmp {

    private static final String mysqlUser = "root";
    private static final String mysqlPWD = "root";
    private static final String jdbcUrl = "jdbc:mysql://192.168.10.200:3306/testdb";
    private static final String jdbcDriver = "com.mysql.jdbc.Driver";

    public static void main(String[] args) throws ParseException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark mysql_to_Hive Example")
                .master("local[*]")
              //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        //连接jdbc数据库
        Properties properties = new Properties();
        properties.put("user", mysqlUser);
        properties.put("password", mysqlPWD);
        properties.put("driver", jdbcDriver);
        //读取数据生成dataset
        Dataset<Row> mysqlToHive = spark.read().jdbc(jdbcUrl, "mysql_to_hive", properties);
        mysqlToHive.createOrReplaceGlobalTempView("mysqlToHive");
        spark.read().table("global_temp.mysqlToHive").show();
    }
}