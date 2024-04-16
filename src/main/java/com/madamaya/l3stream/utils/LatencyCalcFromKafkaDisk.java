package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.runnables.ReadKafkaPartition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class LatencyCalcFromKafkaDisk {
    public static void main(String[] args) throws Exception {
        assert args.length == 3 || args.length == 4;
        String topicName = args[0];
        //String topicName = "test";
        int parallelism = Integer.parseInt(args[1]);
        //int parallelism = 10;
        String outputFileDir = args[2];
        //String outputFileDir = ""
        String key = (args.length == 4) ? args[3] : "";
        //String key = "HO";

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

            // Read all data and calculate latency (1sec median, avg, std)
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
                    Tuple3<Double, Double, Double> calcResult = calcMedianMeanStd(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + calcResult.f0 + "," + calcResult.f1 + "," + calcResult.f2 + "," + latencyListForOneSec.size() + "\n");
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
                    Tuple3<Double, Double, Double> calcResult = calcMedianMeanStd(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + calcResult.f0 + "," + calcResult.f1 + "," + calcResult.f2 + "," + latencyListForOneSec.size() + "\n");
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
            // Calc median latency for all data
            double medianAllLatency = mpq.getMedian();
            long endTime = System.currentTimeMillis();

            /* Get mean & std */
            Tuple2<Double, Double> tuple = calcMeanStd(latencyListForDebug, t -> t.f1, 0.3);
            double mean = tuple.f0;
            double std = tuple.f1;

            // Calc median latency (DEBUG)
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

            // Log
            resultWriter.write("ALL," + medianAllLatency + "," + mean + "," + std + "\n");
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

    public static Tuple3<Double, Double, Double> calcMedianMeanStd(List<Tuple2<Long, Long>> latencyListForOneSec) {
        long count = 0;
        long sum = 0;
        List<Long> latencyValueList = new ArrayList<>();
        for (int i = 0; i < latencyListForOneSec.size(); i++) {
            long val = latencyListForOneSec.get(i).f1;
            latencyValueList.add(val);
            sum += val;
            count += 1;
        }
        latencyValueList.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });

        /* Median calc */
        double median;
        int listSize = latencyValueList.size();
        if (listSize/2 == 0) {
            median = (latencyValueList.get(listSize/2-1) + latencyValueList.get(listSize/2)) / 2.0;
        } else {
            median = (double) latencyValueList.get(listSize/2);
        }

        /* Get mean & std */
        Tuple2<Double, Double> tuple = calcMeanStd(latencyValueList, t -> t);
        return Tuple3.of(median, tuple.f0, tuple.f1);
    }

    public static <T> Tuple2<Double, Double> calcMeanStd(Collection<T> collection, Function<T, Long> func) {
        return calcMeanStd(collection, func, 0);
    }

    public static <T> Tuple2<Double, Double> calcMeanStd(Collection<T> collection, Function<T, Long> func, double filterRate) {
        long count = 0;
        long sum = 0;

        /* Calc mean */
        int idx = 0;
        for (T tuple : collection) {
            if (++idx < collection.size() * filterRate) continue;
            long val = func.apply(tuple);
            sum += val;
            count += 1;
        }
        double mean = sum / (double) count;

        /* Calc std */
        double stdSum = 0;
        idx = 0;
        for (T tuple : collection) {
            if (++idx < collection.size() * filterRate) continue;
            long val = func.apply(tuple);
            stdSum += (val - mean) * (val - mean);
        }
        double var = stdSum / count;
        double std = Math.sqrt(var);

        return Tuple2.of(mean, std);
    }
}
