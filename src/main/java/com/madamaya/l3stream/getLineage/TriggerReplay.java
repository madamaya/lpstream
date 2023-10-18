package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;

public class TriggerReplay {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException();
        }
        String mainPath = args[0];
        String jobid = args[1];
        long outputTs = Long.parseLong(args[2]);
        String lineageTopicName = args[3];
        long maxWindowSize = Long.parseLong(args[4]);
        int numOfSource = Integer.parseInt(args[5]);

        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize, numOfSource);

        // Restart
        String replayCommand = L3Config.BIN_DIR + "/templates/lineage.sh";
        System.out.println("Replay from CpID = " + replayID + " of the job (" + jobid + ")");
        System.out.println("COMMAND --->>> " + replayCommand + " " + L3Config.JAR_PATH + " " + mainPath + " " + jobid + " " + replayID + " " + lineageTopicName);
        Runtime.getRuntime().exec(replayCommand + " " + L3Config.JAR_PATH + " " + mainPath + " " + jobid + " " + replayID + " " + lineageTopicName);
    }
}
