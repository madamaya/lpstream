package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.runnables.IngestKafkaPartition;
import com.madamaya.l3stream.utils.runnables.ShutdownProcessing;

import java.util.HashMap;
import java.util.Map;

public class L3RealtimeLoader {
    public static void main(String[] args) throws Exception {
        assert args.length == 5 || args.length == 6;

        String filePath = args[0];
        String qName = args[1];
        String topic = args[2];
        int parallelism = Integer.parseInt(args[3]);
        int throughput = Integer.parseInt(args[4]);
        int granularity = (args.length == 5) ? 1 : Integer.parseInt(args[5]);
        int datanum = (args.length == 7) ? Integer.parseInt(args[6]) : -1;

        Map<Integer, Double> thMap = new HashMap<>();

        System.out.println("==== ARGS ====");
        System.out.println("\tfilePath = " + filePath);
        System.out.println("\tqName = " + qName);
        System.out.println("\ttopic = " + topic);
        System.out.println("\tparallelism = " + parallelism);
        System.out.println("\tthroughput = " + throughput);
        System.out.println("\tgranularity = " + granularity);
        System.out.println("\tdataNum = " + datanum);
        System.out.println("==============");

        for (int idx = 0; idx < parallelism; idx++) {
            new Thread(new IngestKafkaPartition(filePath, qName, topic, idx, throughput / parallelism, thMap, granularity, datanum / parallelism)).start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownProcessing(filePath, thMap, throughput)));
    }
}