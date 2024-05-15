package com.madamaya.l3stream.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class Sampling {
    public static void main(String[] args) throws Exception {
        assert args.length == 4 || args.length == 5;

        String logDir = args[0];
        int size = Integer.parseInt(args[1]);
        int parallelism = Integer.parseInt(args[2]);
        int numOfSamples = Integer.parseInt(args[3]);
        String samplingStrategy = (args.length==5) ? args[4] : "Random"; // "Random" or "Timestamp"

        // Given: logDir, size, parallelism, numOfSamples, samplingStrategy
        // DO: Read output files, Sample some outputs.
        List<String> sampledOutputs = null;
        if (samplingStrategy == "Random") {
            sampledOutputs = samplingWithRandom(logDir, size, parallelism, numOfSamples);
        } else {
            sampledOutputs = samplingWithTimestamp(logDir, size, parallelism, numOfSamples);
        }

        // Print sampled outputs.
        System.out.println("RESULT");
        for (String output : sampledOutputs) {
            System.out.println(output);
        }

        // Write sampled outputs on a file.
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(logDir + "/" + size + "_sampled.csv"));
            for (String output : sampledOutputs) {
                bw.write(output + "\n");
            }
            bw.close();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private static List<String> samplingWithRandom(String logDir, int size, int parallelism, int numOfSamples) {
        // Map<fileIndex, numOfOutputs>
        int dataNum = 0;
        Map<Integer, Integer> map = new HashMap<>();

        // Count outputs, stored in each file
        try {
            for (int i = 0; i < parallelism; i++) {
                String filePath = logDir + "/" + size + "_" + i + ".csv";
                BufferedReader br = new BufferedReader(new FileReader(filePath));

                int inloop_count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (Integer.MAX_VALUE - 1 < inloop_count) {
                        throw new ArithmeticException();
                    }
                    inloop_count++;
                }
                map.put(i, inloop_count);
                System.out.println(filePath + " + has " + inloop_count + " line(s).");
                if (Integer.MAX_VALUE - inloop_count < dataNum) {
                    throw new ArithmeticException();
                }
                dataNum += inloop_count;
                br.close();
            }
            System.out.println(logDir + "/" + size + "*.csv has " + dataNum + " line(s).");
        } catch (Exception e) {
            System.err.println(e);
        }

        // Decide sampling index and offset
        Random rnd = new Random(137);
        Set<Integer> sampledOutputIndex = new HashSet<>();
        for (int i = 0; i < numOfSamples; i++) {
            sampledOutputIndex.add(rnd.nextInt(dataNum));
        }
        System.out.println("SAMPLED(index): " + sampledOutputIndex);

        // Extract sampled outputs from index and offset
        List<String> sampledOutputs = new ArrayList<>();
        int count = 0;
        try {
            for (int i = 0; i < parallelism && sampledOutputs.size() < numOfSamples; i++) {
                String filePath = logDir + "/" + size + "_" + i + ".csv";
                BufferedReader br = new BufferedReader(new FileReader(filePath));
                int inloop_count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (Integer.MAX_VALUE - 1 < inloop_count) {
                        throw new ArithmeticException();
                    }
                    if (sampledOutputIndex.contains(count + inloop_count)) {
                        // CNFM: デバッグ用
                        // sampledOutputs.add(i + "," + inloop_count + "," + line);
                        String[] elements = line.split(":::::");
                        String ts = elements[1].split(",")[elements[1].split(",").length-1];
                        sampledOutputs.add(i + "," + inloop_count + "," + elements[0] + "," + ts);
                    }
                    inloop_count += 1;

                    if (sampledOutputs.size() >= numOfSamples) break;
                }
                System.out.println("EXTRCT[END]: " + filePath);
                count += inloop_count;
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        return sampledOutputs;
    }

    private static List<String> samplingWithTimestamp(String logDir, int size, int parallelism, int numOfSamples) {
        throw new UnsupportedOperationException();
    }
}
