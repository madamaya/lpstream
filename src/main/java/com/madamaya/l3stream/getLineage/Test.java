package com.madamaya.l3stream.getLineage;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Test {
    public static void main(String[] args) throws Exception {
        // ReplayMonitor.cancelJob();
        Path p = Paths.get("");
        Path p2 = p.toAbsolutePath();
        System.out.println(p2.toString());
    }
}
