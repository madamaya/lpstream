package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.utils.runnables.Monitor;

import java.util.ArrayList;
import java.util.List;

public class ReplayMonitorV2 {
    public static void main(String[] args) throws Exception {
        long outputTs;
        String lineageTopic;
        String outputValue = "";
        String query;
        String size;
        String experimentID;
        int parallelism;

        if (args.length == 7 && args[2].length() > 0) {
            outputTs = Long.parseLong(args[0]);
            lineageTopic = args[1];
            outputValue = args[2];
            query = args[3];
            size = args[4];
            experimentID = args[5];
            parallelism = Integer.parseInt(args[6]);
        } else {
            throw new IllegalArgumentException();
        }
        long startTime = System.currentTimeMillis();

        List<Thread> threadList = new ArrayList<>();
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("MONITOR (create): " + idx);
            threadList.add(new Thread(new Monitor(outputTs, lineageTopic, outputValue, query, size, experimentID, parallelism, idx, startTime)));
        }
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("MONITOR (start): " + idx);
            threadList.get(idx).start();
        }
        System.out.println("MONITOR (isAlive)");
        boolean active = true;
        while (active) {
            for (int idx = 0; idx < parallelism; idx++) {
                if (!threadList.get(idx).isAlive()) {
                    active = false;
                    break;
                }
                Thread.sleep(100);
            }
        }
        for (int idx = 0; idx < parallelism; idx++) {
            if (threadList.get(idx).isAlive()) {
                System.out.println("MONITOR (interrupt): " + idx);
                threadList.get(idx).interrupt();
            }
        }
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("MONITOR (join): " + idx);
            threadList.get(idx).join();
        }
    }
}
