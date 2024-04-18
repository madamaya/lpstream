package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.utils.runnables.ReadKafkaPartition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.util.*;

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

            // Read all data and calculate latency (1sec median, avg, std)
            long currentWindowTime = pq.peek().f1 / 1000;
            List<Tuple2<Long, Long>> latencyListForOneSec = new ArrayList<>(); // Store timestamp ordered data
            MedianCalcWithPQ<Tuple2<Long, Long>> mpq = new MedianCalcWithPQ<>();
            BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputFileDir + "/" + key + ".csv"));
            long sum = 0;
            long count = 0;
            List<Long> sumList = new ArrayList<>();
            while (true) {
                if (pq.isEmpty()) {
                    // Calc 1sec latency (Median)
                    Tuple3<Double, Double, Double> calcResult = calcMedianMeanStd(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + calcResult.f0 + "," + calcResult.f1 + "," + calcResult.f2 + "," + latencyListForOneSec.size() + "\n");
                    // Send all data in 'latencyListForOneSec' to 'latencyListForAll'
                    mpq.appendAll(latencyListForOneSec, t -> t.f1);
                    // Delete all data in 'latencyListForOneSec'
                    latencyListForOneSec.clear();

                    sumList.add(sum);
                    break;
                }

                Tuple3<Integer, Long, Long> tuple = pq.poll();
                int partition = tuple.f0;
                long ts = tuple.f1;
                long latency = tuple.f2;

                // For whole mean calculation
                if (Long.MAX_VALUE - latency <= sum) {
                    sumList.add(sum);
                    sum = 0;
                }
                sum += latency;
                count += 1;

                /* if latency calc should be fired */
                if (ts / 1000 != currentWindowTime) {
                    // Calc 1sec latency (Median)
                    Tuple3<Double, Double, Double> calcResult = calcMedianMeanStd(latencyListForOneSec);
                    // Write the result to file
                    resultWriter.write(currentWindowTime + "," + calcResult.f0 + "," + calcResult.f1 + "," + calcResult.f2 + "," + latencyListForOneSec.size() + "\n");
                    // Update currentWindowTime
                    currentWindowTime = ts / 1000;
                    // Send all data in 'latencyListForOneSec' to 'latencyListForAll'
                    mpq.appendAll(latencyListForOneSec, t -> t.f1);
                    // Delete all data in 'latencyListForOneSec'
                    latencyListForOneSec.clear();
                }

                // Add latest data to 'latencyListForOneSec'
                latencyListForOneSec.add(Tuple2.of(ts, latency));
                // Read a new data from the partition
                String line = brList.get(partition).readLine();
                if (line != null) {
                    String[] elements = line.split(",");
                    Tuple3<Integer, Long, Long> currentTuple = Tuple3.of(Integer.parseInt(elements[0]), Long.parseLong(elements[1]), Long.parseLong(elements[2]));
                    pq.add(currentTuple);
                } else {
                    brList.get(partition).close();
                }
            }
            // Calc mean
            double mean;
            if (sumList.size() == 1) {
                mean = sumList.get(0) / (double) count;
            } else {
                double whole_sum = 0;
                for (long tmpsum : sumList) {
                    whole_sum += tmpsum;
                }
                mean = whole_sum / count;
            }

            // Calc median latency and std for all data
            double medianAllLatency = mpq.getMedian();
            double std = mpq.getStd(mean);

            long endTime = System.currentTimeMillis();

            // Log
            resultWriter.write("ALL," + medianAllLatency + "," + String.format("%.15f", mean) + "," + String.format("%.15f", std) + "\n");
            resultWriter.close();

            System.out.println("Time duration (READ): " + (endTime-writeEndTime) + "[ms]");
            System.out.println("Time duration (ALL): " + (endTime-startTime) + "[ms]");

            logWriter.write("Time duration (READ): " + (endTime-writeEndTime) + "[ms]\n");
            logWriter.write("Time duration (ALL): " + (endTime-startTime) + "[ms]\n");
            logWriter.close();
        } catch (IOException e) {
            System.err.println(e);
        } catch (OutOfMemoryError e) {
            System.err.println(e);
        }
    }

    public static Tuple3<Double, Double, Double> calcMedianMeanStd(List<Tuple2<Long, Long>> latencyListForOneSec) {
        long sum = 0;
        long count = 0;
        MedianCalcWithPQ<Tuple2<Long, Long>> oneSecMPQ = new MedianCalcWithPQ<>();
        for (int i = 0; i < latencyListForOneSec.size(); i++) {
            long latency = latencyListForOneSec.get(i).f1;
            oneSecMPQ.append(latency);

            if (Long.MAX_VALUE - latency > sum) {
                sum += latency;
            } else {
                throw new ArithmeticException();
            }
            count += 1;
        }

        /* Mean */
        double mean = sum / (double) count;

        /* Median */
        double median = oneSecMPQ.getMedian();

        /* Std */
        double std = oneSecMPQ.getStd(mean);

        return Tuple3.of(median, mean, std);
    }
}
