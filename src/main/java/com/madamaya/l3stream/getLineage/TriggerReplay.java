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
        if (args.length != 10) {
            throw new IllegalArgumentException();
        }
        String jarPath = args[0];
        String mainPath = args[1];
        String jobid = args[2];
        long outputTs = Long.parseLong(args[3]);
        String lineageTopicName = args[4];
        long maxWindowSize = Long.parseLong(args[5]);
        int numOfSource = Integer.parseInt(args[6]);
        String query = args[7];
        String size = args[8];
        String experimentID = args[9];

        long startTime = System.currentTimeMillis();
        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize, numOfSource);
        long endTime = System.currentTimeMillis();

        int latencyFlag = 2;
        // Restart
        String replayCommand = L3Config.BIN_DIR + "/templates/lineageReplay.sh";
        System.out.println("Replay from CpID = " + replayID + " of the job (" + jobid + ")");
        System.out.println("COMMAND --->>> " + replayCommand + " " + jarPath + " " + mainPath + " " + L3Config.PARALLELISM + " " + jobid + " " + replayID + " " + lineageTopicName + " " + latencyFlag);
        Runtime.getRuntime().exec(replayCommand + " " + jarPath + " " + mainPath + " " + L3Config.PARALLELISM + " " + jobid + " " + replayID + " " + lineageTopicName + " " + latencyFlag);
        long endTime2 = System.currentTimeMillis();

        BufferedWriter bw;
        try {
            String dataPath = L3Config.L3_HOME + "/data/output/lineage/" + query;
            bw = new BufferedWriter(new FileWriter(dataPath + "/" + size + "-" + "trigger.log", true));
            bw.write(experimentID + "," + startTime + "," + endTime + "," + endTime2 + "," + replayID + "\n");
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
