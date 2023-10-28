package com.blendeer.lab2;

import com.blendeer.SparkDriver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.collect_list;

public class Lab2 {
    public static void main(String[] args) {
        var df = SparkDriver.getSparkSession()
                .read()
                .option("header","true")
//                .schema(minimumDataSchema())
                .option("sep", ";")
                .csv("src/main/resources/succss.csv");
//            .csv("src/main/resources/test.csv");
        df.show();
        df.printSchema();

        df.createOrReplaceTempView("grades");
        Dataset<Row> task1 = SparkDriver.getSparkSession().sql(
                "SELECT CASE " +
                        "WHEN `% відвідувань лекцій`  >= 95 THEN '> 95' " +
                        "WHEN `% відвідувань лекцій` >= 90 AND `% відвідувань лекцій` < 95 THEN '90-95' " +
                        "ELSE '< 90' " +
                        "END AS attendance_range, " +
                        "COLLECT_LIST((`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5) AS average_grades " +
                        "FROM grades " +
                        "GROUP BY attendance_range");
        System.out.println("Відвідування та середні оцінки: ");
        task1.show();
        task1.createOrReplaceTempView("attendance_and_grades");

        Dataset<Row> task2 = SparkDriver.getSparkSession().sql(
                "SELECT CASE " +
                        "WHEN `% відвідувань лекцій`  >= 95 THEN '> 95' " +
                        "WHEN `% відвідувань лекцій` >= 90 AND `% відвідувань лекцій` < 95 THEN '90-95' " +
                        "ELSE '< 90' " +
                        "END AS attendance_range, " +
                        "EXPLODE(average_grades) AS grade " +
                        "FROM (SELECT `% відвідувань лекцій`, COLLECT_LIST((`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5) AS average_grades FROM grades GROUP BY `% відвідувань лекцій`) temp");
        task2 = task2.groupBy("attendance_range").pivot("grade").agg(functions.first("grade"));
        System.out.println("Після розбиття стовпців: ");
        task2.show();

        var task3Count = SparkDriver.getSparkSession().sql("SELECT count(*) FROM grades WHERE " +
                "`Предмет1` >= 90 AND" +
                "`Предмет2` >= 90 AND" +
                "`Предмет3` >= 90 AND" +
                "`Предмет4` >= 90 AND" +
                "`Предмет5` >= 90 " +
                "GROUP BY `Факультет`"
        );
        System.out.println("кількість відмінників на кожному факультеті: " + task3Count);

        var task3Info = SparkDriver.getSparkSession().sql("SELECT * FROM grades WHERE " +
                "`Предмет1` >= 90 AND" +
                "`Предмет2` >= 90 AND" +
                "`Предмет3` >= 90 AND" +
                "`Предмет4` >= 90 AND" +
                "`Предмет5` >= 90 "
        );
        System.out.println("Вивести інформацію про відмінників:" + task3Info);

        var task4 = //SparkDriver.getSparkSession().sql("SELECT g1.`Студент`, g1.`Факультет`, g1.`Предмет1`, g1.`Предмет2`, g1.`Предмет3`, g1.`Предмет4`, g1.`Предмет5`, g1.`% відвідувань лекцій`, g1.average_grades FROM " +
//        "(SELECT *, " +
//        "((`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5) AS average_grades " +
//        "FROM grades) g1 " +
//        "UNION ALL " +
//        "SELECT AVG(((`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5)) FROM  GROUP BY g1.`Факультет`"
//).collect();

                SparkDriver.getSparkSession().sql(
                        "SELECT " +
                                "t1.`Студент`, " +
                                "t1.`Факультет`, " +
                                "t1.`Предмет1`, " +
                                "t1.`Предмет2`, " +
                                "t1.`Предмет3`, " +
                                "t1.`Предмет4`, " +
                                "t1.`Предмет5`, " +
                                "t1.`% відвідувань лекцій` AS `Відвідуваність`, " +
                                "t1.`Середній_бал`, " +
                                "t2.`Середній_бал_факультету`, " +
                                "(t2.`Середній_бал_факультету` - t1.`Середній_бал`) AS `Різниця` " +
                                "FROM (" +
                                "SELECT " +
                                "`Студент`, " +
                                "`Факультет`, " +
                                "`Предмет1`, " +
                                "`Предмет2`, " +
                                "`Предмет3`, " +
                                "`Предмет4`, " +
                                "`Предмет5`, " +
                                "`% відвідувань лекцій`, " +
                                "AVG(`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5 AS `Середній_бал` " +
                                "FROM grades " +
                                "GROUP BY `Студент`, `Факультет`, `Предмет1`, `Предмет2`, `Предмет3`, `Предмет4`, `Предмет5`, `% відвідувань лекцій`" +
                                ") t1 " +
                                "JOIN (" +
                                "SELECT " +
                                "`Факультет`, " +
                                "AVG(`Предмет1` + `Предмет2` + `Предмет3` + `Предмет4` + `Предмет5`) / 5 AS `Середній_бал_факультету` " +
                                "FROM grades " +
                                "GROUP BY `Факультет`" +
                                ") t2 " +
                                "ON t1.`Факультет` = t2.`Факультет`" +
                                "ORDER BY t1.`Студент`"
                );
        System.out.println("Різниця між середнім балом за факультетом та середнім балом поточного студента: ");
        task4.show();
        System.out.println();
    }
}
