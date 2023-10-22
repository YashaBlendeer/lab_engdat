package com.blendeer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Test {
    public static void main(String[] args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss");
        var date = "4/1/2015 16:00:00";
        var res = LocalDateTime.parse(date, formatter);
        System.out.println(res);
    }
}
