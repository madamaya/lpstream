package com.madamaya.l3stream.utils.runnables;

import java.util.Map;

public class ShutdownProcessing implements Runnable {
    private String filePath;
    private Map<Integer, Double> map;
    private int throughput;

    public ShutdownProcessing(String filePath, Map<Integer, Double> map, int throughput) {
        this.filePath = filePath;
        this.map = map;
        this.throughput = throughput;
    }

    @Override
    public void run() {
        double ingest_rate = 0;
        for (double value : map.values()) {
            ingest_rate += value;
        }
        System.out.println("ShutdownProcessing: " + map);
        if (ingest_rate < throughput * 0.999) {
            System.out.println("IngestResult(NG):" + filePath + "," + ingest_rate);
        } else {
            System.out.println("IngestResult(OK):" + filePath + "," + ingest_rate);
        }
    }
}
