package com.madamaya.l3stream.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class Sampling {
    public static void main(String[] args) throws Exception {
        assert args.length == 2;

        String filePath = args[0];
        int numOfSamples = Integer.parseInt(args[1]);

        ObjectMapper om = new ObjectMapper();
        Map<Integer, Tuple2<Long, String>> map = new HashMap<>();
        int cpMax = -1;
        // Count lines in the file
        int count = 0;
        try {
            String line;
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            while ((line = br.readLine()) != null) {
                count++;
                JsonNode jsonNode = om.readTree(line);
                long ts = jsonNode.get("TS").asLong();
                int cpid = jsonNode.get("CPID").asInt();
                Tuple2<Long, String> t2;
                if ((t2 = map.get(cpid)) == null) {
                    map.put(cpid, Tuple2.of(ts, line));
                    cpMax = Math.max(cpMax, cpid);
                } else {
                    if (t2.f0 > ts) {
                        map.put(cpid, Tuple2.of(ts, line));
                        cpMax = Math.max(cpMax, cpid);
                    }
                }
            }
            System.out.println(filePath + " + has " + count + " line(s).");
        } catch (Exception e) {
            System.err.println(e);
        }

        // Decide indexes, radomly sampled (seed = 137)
        Random rand = new Random();
        rand.setSeed(137);

        try {
            int num = 0;
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath + ".target.txt"));

            while (num < numOfSamples) {
                int currentID = rand.nextInt(cpMax + 1);
                Tuple2<Long, String> t2;
                if ((t2 = map.get(currentID)) != null) {
                    bw.write(t2.f1 + "\n");
                    num++;
                }
            }
            bw.flush();
            bw.close();
        } catch (Exception e) {
            System.err.println(e);
        }

        /*
        Set<Integer> set = new HashSet<>();
        while (set.size() < numOfSamples) {
            set.add(rand.nextInt(count));
        }
        System.out.println("Sampled indexes = " + set);

        // Write sampled lines to a file
        int idx = 0;
        try {
            String line;
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath + ".target.txt"));
            while ((line = br.readLine()) != null) {
                if (set.contains(idx++)) {
                    bw.write(line + "\n");
                    bw.flush();
                }
            }
            bw.close();
        } catch (Exception e) {
            System.err.println(e);
        }
         */
    }
}
