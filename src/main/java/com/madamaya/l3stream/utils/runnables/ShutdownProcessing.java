package com.madamaya.l3stream.utils.runnables;

import java.util.Map;

public class ShutdownProcessing implements Runnable {
    private Map<Integer, Double> map;

    public ShutdownProcessing(Map<Integer, Double> map) {
        this.map = map;
    }

    @Override
    public void run() {
        System.out.println(map);
    }
}
