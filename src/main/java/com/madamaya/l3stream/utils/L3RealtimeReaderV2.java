package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.runnables.ReadKafkaPartition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class L3RealtimeLoaderV2 {
    public static void main(String[] args) throws Exception {
        assert args.length == 3 || args.length == 4;
        String topicName = args[0];
        int parallelism = Integer.parseInt(args[1]);
        String outputFileDir = args[2];
        String key = (args.length == 4) ? args[3] : "";

        System.out.println("==== ARGS ====");
        System.out.println("\ttopicName = " + topicName);
        System.out.println("\tparallelism = " + parallelism);
        System.out.println("\toutputFileDir = " + outputFileDir);
        System.out.println("\tkey = " + key);
        System.out.println("==============");
        try {
            BufferedWriter logWriter = new BufferedWriter(new FileWriter(outputFileDir + "/" + key + "_log.csv"));
            long startTime = System.currentTimeMillis();
            /* Write all output on files */
            List<Thread> threadList = new ArrayList<>();
            for (int idx = 0; idx < parallelism; idx++) {
                System.out.println("ADD: " + idx);
                threadList.add(new Thread(new ReadKafkaPartition(topicName, idx, outputFileDir, key)));
            }
            for (int idx = 0; idx < parallelism; idx++) {
                System.out.println("START: " + idx);
                threadList.get(idx).start();
            }
            for (int idx = 0; idx < parallelism; idx++) {
                System.out.println("JOIN: " + idx);
                threadList.get(idx).join();
            }
            long writeEndTime = System.currentTimeMillis();
            System.out.println("WRITE END");
            System.out.println("Time duration (WRITE): " + (writeEndTime - startTime) + "[ms]");
            logWriter.write("Time duration (WRITE): " + (writeEndTime - startTime) + "[ms]\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
