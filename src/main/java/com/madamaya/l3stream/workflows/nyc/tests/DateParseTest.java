package com.madamaya.l3stream.workflows.nyc.tests;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateParseTest {
    public static void main(String[] args) {
        String str = "2018-02-01 00:01:58.100";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            System.out.println(str);
            Date date = sdf.parse(str);
            System.out.println(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            System.out.println(calendar.getTimeInMillis());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
