package com.madamaya.l3stream.cpstore;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.flink.api.common.JobID;

public class CpManagementConfig {
    @Parameter(names = "--checkpointDir", required = true)
    private String checkpointDir;
    @Parameter(names = "--jobID")
    private JobID jobID;
    @Parameter(names = "--CpMServerIP")
    private String CpMServerIP = "localhost";
    @Parameter(names = "--CpMServerPort")
    private int CpMServerPort = 10010;

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public void setCheckpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    public JobID getJobID() {
        return jobID;
    }

    public void setJobID(JobID jobID) {
        this.jobID = jobID;
    }

    public String getCpMServerIP() {
        return CpMServerIP;
    }

    public void setCpMServerIP(String cpMServerIP) {
        CpMServerIP = cpMServerIP;
    }

    public int getCpMServerPort() {
        return CpMServerPort;
    }

    public void setCpMServerPort(int cpMServerPort) {
        CpMServerPort = cpMServerPort;
    }

    public static CpManagementConfig newInstance(String[] args) {
        CpManagementConfig config = new CpManagementConfig();
        JCommander.newBuilder().addObject(config).build().parse(args);
        return config;
    }
}
