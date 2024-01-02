package com.madamaya.l3stream.workflows.syn1.objects;

public class SynPowerTuple extends SynInputTuple {
    private int machineId;
    private double power;
    private String log;
    private long timestamp;
    private long stimulus;

    public SynPowerTuple(int type) {
        super(type);
    }

    public SynPowerTuple(int type, int machineId, double power, String log, long timestamp, long stimulus) {
        super(type);
        this.machineId = machineId;
        this.power = power;
        this.log = log;
        this.timestamp = timestamp;
        this.stimulus = stimulus;
    }

    public SynPowerTuple(int type, int machineId, double power, String log, long timestamp) {
        super(type);
        this.machineId = machineId;
        this.power = power;
        this.log = log;
        this.timestamp = timestamp;
    }

    public int getMachineId() {
        return machineId;
    }

    public void setMachineId(int machineId) {
        this.machineId = machineId;
    }

    public double getPower() {
        return power;
    }

    public void setPower(double power) {
        this.power = power;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "SynPowerTuple{" +
                "machineId=" + machineId +
                ", power=" + power +
                ", log='" + log + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
