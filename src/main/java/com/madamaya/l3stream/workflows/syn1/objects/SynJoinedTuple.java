package com.madamaya.l3stream.workflows.syn1.objects;

public class SynJoinedTuple {
    private int machineId;
    private int sensorId;
    private double temperature;
    private double power;
    private String logTemp;
    private String logPower;
    private long timestamp;
    private long stimulus;

    public SynJoinedTuple(int machineId, int sensorId, double temperature, double power, String logTemp, String logPower, long timestamp, long stimulus) {
        this.machineId = machineId;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.power = power;
        this.logTemp = logTemp;
        this.logPower = logPower;
        this.timestamp = timestamp;
        this.stimulus = stimulus;
    }

    public SynJoinedTuple(int machineId, int sensorId, double temperature, double power, String logTemp, String logPower, long timestamp) {
        this.machineId = machineId;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.power = power;
        this.logTemp = logTemp;
        this.logPower = logPower;
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

    public double getPower() {
        return power;
    }

    public void setPower(double power) {
        this.power = power;
    }

    public String getLogTemp() {
        return logTemp;
    }

    public void setLogTemp(String logTemp) {
        this.logTemp = logTemp;
    }

    public String getLogPower() {
        return logPower;
    }

    public void setLogPower(String logPower) {
        this.logPower = logPower;
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
        return "SynJoinedTuple{" +
                "machineId=" + machineId +
                ", sensorId=" + sensorId +
                ", temperature=" + temperature +
                ", power=" + power +
                ", logTemp='" + logTemp + '\'' +
                ", logPower='" + logPower + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
