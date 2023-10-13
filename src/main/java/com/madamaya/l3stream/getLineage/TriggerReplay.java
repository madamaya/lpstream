package com.madamaya.l3stream.getLineage;

public class TriggerReplay {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException();
        }
        String jobid = args[0];
        long outputTs = Long.parseLong(args[1]);
        long maxWindowSize = Long.parseLong(args[2]);

        String jobName = FindJobName.getJobName(jobid);
        String queryName = jobName.split(",")[1].toLowerCase();
        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize);

        // Restart
        String binPath = System.getProperty("user.dir");
        Runtime.getRuntime().exec(binPath + "/" + queryName + "/l3" + queryName + "FromState.sh " + jobid + " " + replayID);
    }
}
