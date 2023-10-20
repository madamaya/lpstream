package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;

public class TriggerReplay {
    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            throw new IllegalArgumentException();
        }
        String jarPath = args[0];
        String mainPath = args[1];
        String jobid = args[2];
        long outputTs = Long.parseLong(args[3]);
        String lineageTopicName = args[4];
        long maxWindowSize = Long.parseLong(args[5]);
        int numOfSource = Integer.parseInt(args[6]);

        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize, numOfSource);

        // Restart
        String replayCommand = L3Config.BIN_DIR + "/templates/lineage.sh";
        System.out.println("Replay from CpID = " + replayID + " of the job (" + jobid + ")");
        System.out.println("COMMAND --->>> " + replayCommand + " " + jarPath + " " + mainPath + " " + jobid + " " + replayID + " " + lineageTopicName);
        Runtime.getRuntime().exec(replayCommand + " " + jarPath + " " + mainPath + " " + jobid + " " + replayID + " " + lineageTopicName);
    }
}
