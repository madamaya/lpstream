package com.madamaya.l3stream.workflows.nexmark.tests;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTest {
    public static void main(String[] args) throws Exception {
        String str = "2023-10-03 05:31:34.";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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

        System.out.println("NexmarkInputTuple.convertDateStrToLong(str)");
        System.out.println(NexmarkInputTuple.convertDateStrToLong(str));
    }
}
