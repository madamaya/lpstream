package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.l3stream.conf.L3conf;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TriggerReplay {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        if (args.length != 10 && args.length != 11) {
            throw new IllegalArgumentException();
        }
        String jarPath = args[0];
        String mainPath = args[1];
        String jobid = args[2];
        long outputTs = Long.parseLong(args[3]);
        String lineageTopicName = args[4];
        long maxWindowSize = Long.parseLong(args[5]);
        int numOfSource = Integer.parseInt(args[6]);
        String experimentName = args[7];
        String aggregateStrategy = args[8];
        int CPID = Integer.parseInt(args[9]);
        String windowSize = (args.length == 11) ? args[10] : "";
        int latencyFlag = 2;

        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize, numOfSource);

        long endTime = System.currentTimeMillis();

        // Restart
        String replayCommand = L3Config.BIN_DIR + "/templates/lineageReplay.sh";
        System.out.println("Replay from CpID = " + replayID + " of the job (" + jobid + ")");
        System.out.println("COMMAND --->>> " + replayCommand + " " + jarPath + " " + mainPath + " " + L3Config.PARALLELISM + " " + jobid + " " + replayID + " " + lineageTopicName + " " + latencyFlag + " " + aggregateStrategy + " " + windowSize);
        Runtime.getRuntime().exec(replayCommand + " " + jarPath + " " + mainPath + " " + L3Config.PARALLELISM + " " + jobid + " " + replayID + " " + lineageTopicName + " " + latencyFlag + " " + aggregateStrategy + " " + windowSize);

        long endTime2 = System.currentTimeMillis();

        BufferedWriter bw;
        try {
            String[] elements = experimentName.split("-");
            String query = elements[0];
            String id = elements[1];
            String dataPath = L3Config.L3_HOME + "/data/output/metrics34/" + query;
            if (Files.notExists(Paths.get(dataPath))) {
                Files.createDirectories(Paths.get(dataPath));
            }
            bw = new BufferedWriter(new FileWriter(dataPath + "/" + id + "-" + windowSize + "-" + "trigger.log"));
            bw.write(startTime + "," + endTime + "," + endTime2 + "," + CPID + "," + replayID);
            bw.flush();
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
