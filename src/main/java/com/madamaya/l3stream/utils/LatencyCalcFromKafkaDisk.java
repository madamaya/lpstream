package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.runnables.ReadKafkaPartition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class LatencyCalcFromKafkaDisk {
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
            System.out.println("Time duration (WRITE): " + (writeEndTime-startTime) + "[ms]");
            logWriter.write("Time duration (WRITE): " + (writeEndTime-startTime) + "[ms]\n");

            /* Read data (Make data sequence timestamp ordered.) */
            PriorityQueue<Tuple3<Integer, Long, Long>> pq = new PriorityQueue<>(new Comparator<Tuple3<Integer, Long, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Long, Long> o1, Tuple3<Integer, Long, Long> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
            List<BufferedReader> brList = new ArrayList<>();
            for (int idx = 0; idx < parallelism; idx++) {
                brList.add(new BufferedReader(new FileReader(outputFileDir + "/" + key + "_" + idx + ".csv")));
                String line = brList.get(idx).readLine();

                // read first line of each file and init pq
                if (line != null) {
                    String[] elements = line.split(",");
                    Tuple3<Integer, Long, Long> currentTuple = Tuple3.of(Integer.parseInt(elements[0]), Long.parseLong(elements[1]), Long.parseLong(elements[2]));
                    pq.add(currentTuple);
                }
            }

            // Read all data and calculate latency (1sec median)
            // ''Assume that all latency values can be on memory.''
            // ''If not so, another implementation is needed.''
            long currentWindowTime = pq.peek().f1 / 1000;
            List<Tuple2<Long, Long>> latencyListForOneSec = new ArrayList<>(); // Store timestamp ordered data
            List<Tuple2<Long, Long>> latencyListForDebug = new ArrayList<>();
            MedianCalcWithPQForTuple2 mpq = new MedianCalcWithPQForTuple2();
            BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputFileDir + "/" + key + ".csv"));
            while (true) {
                if (pq.isEmpty()) {
                    // Calc 1sec latency (Median)
                    double medianLatency = calcMedian(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + medianLatency + "," + latencyListForOneSec.size() + "\n");
                    // Send all data in 'latencyListForOneSec' to 'latencyListForAll'
                    mpq.appendAll(latencyListForOneSec);

                    latencyListForDebug.addAll(latencyListForOneSec);

                    // Delete all data in 'latencyListForOneSec'
                    latencyListForOneSec.clear();
                    break;
                }

                Tuple3<Integer, Long, Long> tuple = pq.poll();
                int partition = tuple.f0;
                long ts = tuple.f1;
                long latency = tuple.f2;

                /* if latency calc should be fired */
                if (ts / 1000 != currentWindowTime) {
                    // Calc 1sec latency (Median)
                    double medianLatency = calcMedian(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + medianLatency + "," + latencyListForOneSec.size() + "\n");
                    // Update currentWindowTime
                    currentWindowTime = ts / 1000;
                    // Send all data in 'latencyListForOneSec' to 'latencyListForAll'
                    mpq.appendAll(latencyListForOneSec);

                    latencyListForDebug.addAll(latencyListForOneSec);

                    // Delete all data in 'latencyListForOneSec'
                    latencyListForOneSec.clear();
                }

                // add latest data to 'latencyListForOneSec' and read a new data from the partition
                latencyListForOneSec.add(Tuple2.of(ts, latency));
                String line = brList.get(partition).readLine();
                if (line != null) {
                    String[] elements = line.split(",");
                    Tuple3<Integer, Long, Long> currentTuple = Tuple3.of(Integer.parseInt(elements[0]), Long.parseLong(elements[1]), Long.parseLong(elements[2]));
                    pq.add(currentTuple);
                } else {
                    brList.get(partition).close();
                }
            }
            // Calc latency DEBUG
            latencyListForDebug.sort(new Comparator<Tuple2<Long, Long>>() {
                @Override
                public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
            double medianAllLatencyDebug = -1;
            if (latencyListForDebug.size()%2 == 0) {
                medianAllLatencyDebug = (latencyListForDebug.get(latencyListForDebug.size()/2-1).f1 + latencyListForDebug.get(latencyListForDebug.size()/2).f1) / 2.0;
            } else {
                medianAllLatencyDebug = latencyListForDebug.get(latencyListForDebug.size()/2).f1;
            }
            resultWriter.write("DEBUG," + medianAllLatencyDebug + "\n");

            // Calc latency for all data
            double medianAllLatency = mpq.getMedian();
            long endTime = System.currentTimeMillis();

            // Log
            resultWriter.write("ALL," + medianAllLatency + "\n");
            resultWriter.close();

            System.out.println("Time duration (READ): " + (endTime-writeEndTime) + "[ms]");
            System.out.println("Time duration (ALL): " + (endTime-startTime) + "[ms]");

            logWriter.write("Time duration (READ): " + (endTime-writeEndTime) + "[ms]\n");
            logWriter.write("Time duration (ALL): " + (endTime-startTime) + "[ms]\n");
            logWriter.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public static double calcMedian(List<Tuple2<Long, Long>> latencyListForOneSec) {
        List<Long> latencyValueList = new ArrayList<>();
        for (int i = 0; i < latencyListForOneSec.size(); i++) {
            latencyValueList.add(latencyListForOneSec.get(i).f1);
        }
        latencyValueList.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });

        int listSize = latencyValueList.size();
        if (listSize/2 == 0) {
            return (latencyValueList.get(listSize/2-1) + latencyValueList.get(listSize/2)) / 2.0;
        } else {
            return (double) latencyValueList.get(listSize/2);
        }
    }
}
