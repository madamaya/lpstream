package com.madamaya.l3stream.workflows.syn1.objects;

public class SynTempTuple extends SynInputTuple {
    private int machineId;
    private int sensorId;
     private double temperature;
    private String log;
    private long timestamp;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public SynTempTuple(int type) {
        super(type);
    }

    public SynTempTuple(int type, int machineId, int sensorId, double temperature, String log, long timestamp, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        super(type);
        this.machineId = machineId;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.log = log;
        this.timestamp = timestamp;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
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

    public SynTempTuple(SynTempTuple tuple) {
        super(tuple.getType());
        this.machineId = tuple.getMachineId();
        this.sensorId = tuple.getSensorId();
        this.temperature = tuple.getTemperature();
        this.log = tuple.getLog();
        this.timestamp = tuple.getTimestamp();
        this.dominantOpTime = tuple.getDominantOpTime();
        this.kafkaAppendTime = tuple.getKafkaAppendTime();
        this.stimulus = tuple.getStimulus();
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

    public long getDominantOpTime() {
        return dominantOpTime;
    }

    public void setDominantOpTime(long dominantOpTime) {
        this.dominantOpTime = dominantOpTime;
    }

    public long getKafkaAppendTime() {
        return kafkaAppendTime;
    }

    public void setKafkaAppendTime(long kafkaAppendTime) {
        this.kafkaAppendTime = kafkaAppendTime;
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
