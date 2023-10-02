package com.blendeer;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class SparkDriver implements Serializable {

    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("lab")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .master("local")
                .getOrCreate();

    }
}