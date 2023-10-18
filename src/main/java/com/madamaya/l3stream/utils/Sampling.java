package com.madamaya.l3stream.utils;

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

        // Count lines in the file
        int count = 0;
        try {
            String line;
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            while ((line = br.readLine()) != null) {
                count++;
            }
            System.out.println(filePath + " + has " + count + " line(s).");
        } catch (Exception e) {
            System.err.println(e);
        }

        // Decide indexes, radomly sampled (seed = 137)
        Random rand = new Random();
        rand.setSeed(137);

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
    }
}
