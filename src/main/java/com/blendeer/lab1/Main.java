package com.blendeer.lab1;

import com.blendeer.SparkDriver;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {

  public static void main(String[] args) {

    var df = SparkDriver.getSparkSession()
            .read()
            .option("header","true")
            .schema(minimumDataSchema())
            .csv("src/main/resources/Microsoft_Stock.csv");
//            .csv("src/main/resources/test.csv");
    df.show();
    df.printSchema();

    var bean = Encoders.javaSerialization(LabData.class);
    Dataset<LabData> ds = df.map(
            new DataMapper(),
            bean
    );
    var resultList = ds.collectAsList();


    /*1) Знайти середню ціну акцій на час відкриття торгів для кожного року.
      2) Вивести кількість днів, коли торги відкрились не о 16:00.
      3) Відсортувати дані за кожен рік за об’ємом продажів.
      4) Порахувати середньоквадратичне відхилення ціни на час закриття для кожного місяця 2015-го року.*/

    var averageOpenCostYearly = resultList.stream()
            .collect(Collectors.groupingBy(i -> i.getDate().getYear(),  Collectors.averagingDouble(LabData::getOpen)));
    var notWhen16Hours = resultList.stream()
            .filter(d -> d.getDate().getHour() != 16)
            .count();
    var sortedByYearsVolume = resultList.stream()
            .sorted(Comparator.comparing(LabData::getDate, Comparator.comparing(LocalDateTime::getYear)).thenComparing(LabData::getVolume))
            .toList();
    var sdMonthly2015 = resultList.stream()
            .filter(r -> r.getDate().getYear() == 2015)
            .collect(Collectors.groupingBy(i -> i.getDate().getMonth(),  Collectors.mapping(LabData::getClose, Collectors.toList())))
            .entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> calculateStandardDeviation(e.getValue())));

    System.out.println("Знайти середню ціну акцій на час відкриття торгів для кожного року: " + averageOpenCostYearly);
    System.out.println("Вивести кількість днів, коли торги відкрились не о 16:00: " + notWhen16Hours);
    System.out.println("Відсортувати дані за кожен рік за об’ємом продажів: " + sortedByYearsVolume);
    System.out.println("Порахувати середньоквадратичне відхилення ціни на час закриття для кожного місяця 2015-го року: " + sdMonthly2015);

    var row = SparkDriver.getSparkSession().createDataset(
            List.of(new LabData(LocalDateTime.now(),
                    333.4,
                    4.0,
                    4.0,
                    5.0,
                    666)), bean);
    ds.unionAll(row);
    var newDF = ds.toDF();
    //df.filter("Volume > 27347758").collect()

    System.out.println(df);
  }

  @Contract(" -> new")
  public static @NotNull StructType minimumDataSchema() {
    return DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("Date", DataTypes.StringType, true),
            DataTypes.createStructField("Open", DataTypes.DoubleType, true),
            DataTypes.createStructField("High", DataTypes.DoubleType, true),
            DataTypes.createStructField("Low", DataTypes.DoubleType, true),
            DataTypes.createStructField("Close", DataTypes.DoubleType, true),
            DataTypes.createStructField("Volume", DataTypes.LongType, true) }
    );
  }

  public static double calculateStandardDeviation(List<Double> list) {
    Double[] array = list.toArray(Double[]::new);
    // get the sum of array
    double sum = 0.0;
    for (double i : array) {
      sum += i;
    }

    // get the mean of array
    int length = array.length;
    double mean = sum / length;

    // calculate the standard deviation
    double standardDeviation = 0.0;
    for (double num : array) {
      standardDeviation += Math.pow(num - mean, 2);
    }

    return Math.sqrt(standardDeviation / length);
  }

}
