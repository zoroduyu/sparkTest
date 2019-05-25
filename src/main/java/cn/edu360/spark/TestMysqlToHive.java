package cn.edu360.spark;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestMysqlToHive {

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
        RDD<Row> rdd = mysqlToHive.rdd();
        //创建数据库
        //String createDBSQL = "CREATE DATABASE IF NOT EXISTS " + hiveDBName + " LOCATION '" + dbPath + "'";
        //sparkSession.sql(createDBSQL);
        mysqlToHive.cache();
        //读取mysql的表结构
        StructType structType = mysqlToHive.schema();
        StructField[] structFields = structType.fields();
        mysqlToHive.registerTempTable("mysql_to_hive_tmp");
        spark.sql("use test_hive");
        spark.sql("CREATE TABLE IF NOT EXISTS mysql_to_hive_spark  (id STRING , name string, value string) ");
        spark.sql("insert into mysql_to_hive_spark select id, name ,value from mysql_to_hive_tmp ").show();



        /*
        structField是StructType中的字段。
        param：name此字段的名称。
        param：dataType此字段的数据类型。
        param：nullable指示此字段的值是否为空值。
        param：metadata此字段的元数据。 如果未修改列的内容（例如，在选择中），则应在转换期间保留元数据。
         */
//        String sourceType; //Name of the type used in JSON serialization.
//        String columnName;
//        String targetType;
//        StructField structField;
//        StringBuilder createBuilder = new StringBuilder();
//        createBuilder.append("CREATE TABLE IF NOT EXISTS ").append(realHiveTableName).append(" (");
//        List<String> dbTableColumns = Lists.newArrayList();
//        Map<String, String> dbTableColumnTypeMap = Maps.newHashMap();

//        HiveUtil connectionToHive = new HiveUtil("org.apache.hive.jdbc.HiveDriver", hiveUrl, hiveUser, hivePassword);

    }
}