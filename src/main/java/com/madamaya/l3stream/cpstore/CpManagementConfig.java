package com.madamaya.l3stream.cpstore;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.flink.api.common.JobID;

public class CpManagementConfig {
    @Parameter(names = "--checkpointDir", required = true)
    private String checkpointDir;
    @Parameter(names = "--jobID")
    private JobID jobID;
    @Parameter(names = "--ip")
    private String ip = "localhost";
    @Parameter(names = "--port")
    private int port = 10010;

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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public static CpManagementConfig newInstance(String[] args) {
        CpManagementConfig config = new CpManagementConfig();
        JCommander.newBuilder().addObject(config).build().parse(args);
        return config;
    }
}
