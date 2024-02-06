package com.madamaya.l3stream.workflows.syn1.objects;

public class SynPowerTuple extends SynInputTuple {
    private int machineId;
    private double power;
    private String log;
    private long timestamp;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public SynPowerTuple(int type) {
        super(type);
    }

    public SynPowerTuple(int type, int machineId, double power, String log, long timestamp, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        super(type);
        this.machineId = machineId;
        this.power = power;
        this.log = log;
        this.timestamp = timestamp;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public SynPowerTuple(int type, int machineId, double power, String log, long timestamp) {
        super(type);
        this.machineId = machineId;
        this.power = power;
        this.log = log;
        this.timestamp = timestamp;
    }

    public SynPowerTuple(SynPowerTuple tuple) {
        super(tuple.getType());
        this.machineId = tuple.getMachineId();
        this.power = tuple.getPower();
        this.log = tuple.getLog();
        this.timestamp = tuple.getTimestamp();
        this.kafkaAppendTime = tuple.getKafkaAppendTime();
        this.stimulus = tuple.getStimulus();
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
        return "SynPowerTuple{" +
                "machineId=" + machineId +
                ", power=" + power +
                ", log='" + log + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
