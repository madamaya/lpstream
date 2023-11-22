package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.parseFunc.*;
import com.madamaya.l3stream.utils.runnables.MyTestRun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class L3RealtimeLoaderTest {
    public static void main(String[] args) throws Exception {
        assert args.length == 4;

        String filePath = args[0];
        String qName = args[1];
        String topic = args[2];
        int parallelism = Integer.parseInt(args[3]);

        System.out.println("==== ARGS ====");
        System.out.println("\tfilePath = " + filePath);
        System.out.println("\tqName = " + qName);
        System.out.println("\ttopic = " + topic);
        System.out.println("\tparallelism = " + parallelism);
        System.out.println("==============");

        Map<Integer, Long> map = new HashMap<>();
        for (int i = 0; i < parallelism; i++) {
            new Thread(new MyTestRun(filePath, qName, topic, i, map)).start();
        }
    }
}
