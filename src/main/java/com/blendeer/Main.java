package com.blendeer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

  public static void main(String[] args) {

//    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
//    JavaSparkContext context = new JavaSparkContext(conf);
//
//    JavaRDD<String> stringJavaRDD = context.textFile("src/main/resources/temp.txt");
//    System.out.println(stringJavaRDD.count());

//    Dataset<Row> df = SparkDriver.getSparkSession()
    var df = SparkDriver.getSparkSession()
            .read()
            .csv("src/main/resources/test.csv");
    df.select("_c0", "_c1").collectAsList();
    System.out.println(df);
  }

//  public static StructType minimumDataSchema() {
//    return DataTypes.createStructType(new StructField[] {
//            DataTypes.createStructField("Date", DataTypes.StringType, true),
//            DataTypes.createStructField("Open", DataTypes.StringType, true),
//            DataTypes.createStructField("High", DataTypes.StringType, true),
//            DataTypes.createStructField("Low", DataTypes.StringType, true),
//            DataTypes.createStructField("Close", DataTypes.StringType, true),
//            DataTypes.createStructField("Volume", DataTypes.IntegerType, true) }
//    );
//  }

}
