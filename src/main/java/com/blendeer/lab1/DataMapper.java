package com.blendeer.lab1;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class DataMapper implements MapFunction<Row, LabData> {
    @Override
    public LabData call(Row row) {
        LabData customer = new LabData();
        customer.setDate(row.getAs("Date"));
        customer.setOpen(row.getAs("Open"));
        customer.setHigh(row.getAs("High"));
        customer.setLow(row.getAs("Low"));
        customer.setClose(row.getAs("Close"));
        customer.setVolume(row.getAs("Volume"));
        return customer;
    }
}
