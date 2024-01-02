package com.madamaya.l3stream.workflows.syn1.objects;

public class SynTempTuple extends SynInputTuple {
    private int machineId;
    private int sensorId;
     private double temperature;
    private String log;
    private long timestamp;
    private long stimulus = Long.MAX_VALUE;

    public SynTempTuple(int type) {
        super(type);
    }

    public SynTempTuple(int type, int machineId, int sensorId, double temperature, String log, long timestamp, long stimulus) {
        super(type);
        this.machineId = machineId;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.log = log;
        this.timestamp = timestamp;
        this.stimulus = stimulus;
    }

    public SynTempTuple(int type, int machineId, int sensorId, double temperature, String log, long timestamp) {
        super(type);
        this.machineId = machineId;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.log = log;
        this.timestamp = timestamp;
    }

    public int getMachineId() {
        return machineId;
    }

    public void setMachineId(int machineId) {
        this.machineId = machineId;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
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
        return "SynInputTuple{" +
                "machineId=" + machineId +
                ", sensorId=" + sensorId +
                ", temperature=" + temperature +
                ", log='" + log + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
