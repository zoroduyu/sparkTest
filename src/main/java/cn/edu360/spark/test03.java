package cn.edu360.spark;


import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class test03 {
    public static void main(String[] args) throws ParseException {
    	
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
              //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

//        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
//        //创建sparkContext
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//        HiveContext hiveCtx = new HiveContext(jsc);
//        SQLContext sqlCtx = new SQLContext(jsc);
//        SparkSession spark = new SparkSession(jsc);
        //加载数据到rdd
//        JavaRDD<String> lines = jsc.textFile("D:\\key\\test.txt");
        Dataset<Row> csv = spark.read().csv("file:///D:\\key\\test.txt");
        csv.show();
////        csv.show();
////        Dataset<Row> filter = csv.filter(" ");
////        filter.show();
        JavaRDD<Row> rowJavaRDD = csv.javaRDD();
        List<Row> collect = rowJavaRDD.collect();
//        for(Row row : collect){
//            Object o = row.get(0);
//            System.out.print("得到一个数据"+o);
//        }
//        System.out.print("数据"+rowJavaRDD.count());
        //动态的构建DataFrame的元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField( "id", DataTypes.StringType, true ));
//        structFields.add(DataTypes.createStructField( "name", DataTypes.StringType, true ));
//        structFields.add(DataTypes.createStructField( "value", DataTypes.StringType, true ));
        //构建StructType用于DataFrame 元数据的描述
        StructType structType = DataTypes.createStructType( structFields );

        //基于MeataData以及RDD<Row>来构造DataFrame
//        Dataset<Row> personsDF = spark.createDataFrame(collect,Row.class);
//        personsDF.show();
//        personsDF.registerTempTable("brands_tmp");
        Dataset<Row> dataFrame = spark.createDataFrame(collect, structType);
        dataFrame.registerTempTable("brands_tmp");
        dataFrame.write().csv("file:///D:\\key\\test.txt");
//        hiveCtx.sql("use test_hive");
//        //删除原来的表
//        hiveCtx.sql("drop table if exists sousuo.temp_yeqingyun_20170913");
//        //创建表
//        hiveCtx.sql("CREATE TABLE IF NOT EXISTS sousuo.temp_yeqingyun_20170913 (brand STRING)");
//        //将brands表中的内容全部拷贝到temp_yeqingyun_20170913表中
//        hiveCtx.sql("insert into sousuo.temp_yeqingyun_20170913 select brand from brands");
        spark.sql("use test_hive");
//        spark.sql("select * from brands").show();
        //spark.sql("SELECT * FROM mysql_to_hive").show();
        spark.sql("drop table if exists brands ");
        spark.sql("CREATE TABLE IF NOT EXISTS brands  (id STRING)");
        spark.sql("insert into brands select id from brands_tmp");




    }
}