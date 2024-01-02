package com.madamaya.l3stream.workflows.syn1.objects;

public class SynResultTuple {
    private int machineId;
    private double avgTemp;
    private long timestamp;
    private long stimulus;

    public SynResultTuple(int machineId, double avgTemp, long timestamp, long stimulus) {
        this.machineId = machineId;
        this.avgTemp = avgTemp;
        this.timestamp = timestamp;
        this.stimulus = stimulus;
    }

    public SynResultTuple(int machineId, double avgTemp, long timestamp) {
        this.machineId = machineId;
        this.avgTemp = avgTemp;
        this.timestamp = timestamp;
    }

    public int getMachineId() {
        return machineId;
    }

    public void setMachineId(int machineId) {
        this.machineId = machineId;
    }

    public double getAvgTemp() {
        return avgTemp;
    }

    public void setAvgTemp(double avgTemp) {
        this.avgTemp = avgTemp;
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
        return "SynResultTuple{" +
                "machineId=" + machineId +
                ", avgTemp=" + avgTemp +
                ", timestamp=" + timestamp +
                '}';
    }
}
