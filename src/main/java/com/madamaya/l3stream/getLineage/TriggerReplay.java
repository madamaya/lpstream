package com.madamaya.l3stream.getLineage;

public class TriggerReplay {
    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            throw new IllegalArgumentException();
        }
        String mainPath = args[0];
        String jobid = args[1];
        long outputTs = Long.parseLong(args[2]);
        String lineageTopicName = args[3];
        long maxWindowSize = Long.parseLong(args[4]);
        int parallelism = Integer.parseInt(args[5]);
        int numOfSource = Integer.parseInt(args[6]);
        String redisIP = args[7];
        int redisPort = Integer.parseInt(args[8]);

        String jobName = FindJobName.getJobName(jobid);
        String queryName = jobName.split(",")[1].toLowerCase();
        int replayID = FindReplayCPID.getReplayID(outputTs, maxWindowSize, parallelism, numOfSource, redisIP, redisPort);

        // Restart
        String binPath = System.getProperty("user.dir");
        System.out.println("Replay from CpID = " + replayID + " of the job (" + jobid + ")");
        System.out.println("COMMAND --->>> " + binPath + "/templates/lineage.sh " + mainPath + " " +jobid + " " + replayID + " " + parallelism + " " + lineageTopicName);
        Runtime.getRuntime().exec(binPath + "/templates/lineage.sh " + mainPath + " " +jobid + " " + replayID + " " + parallelism + " " + lineageTopicName);
    }
}
