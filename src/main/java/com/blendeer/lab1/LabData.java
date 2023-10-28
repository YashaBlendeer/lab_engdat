package com.blendeer.lab1;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LabData implements Serializable {
    private LocalDateTime date;
    private double open;
    private double high;
    private double low;
    private double close;
    private long volume;

    public void setDate(String date) {
        // 4/1/2015 16:00:00
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss");
        this.date = LocalDateTime.parse(date, formatter);
    }

}
