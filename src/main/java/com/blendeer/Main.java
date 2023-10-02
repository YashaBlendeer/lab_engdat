package com.blendeer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> stringJavaRDD = context.textFile("C:\\E\\smth\\un\\4 course\\endat\\lab\\src\\main\\resources\\temp.txt");
    System.out.println(stringJavaRDD.count());
  }
}
