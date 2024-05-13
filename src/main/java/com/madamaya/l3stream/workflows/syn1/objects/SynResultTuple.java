package com.madamaya.l3stream.workflows.syn1.objects;

public class SynResultTuple {
    private int machineId;
    private double avgTemp;
    private long timestamp;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    // CNFM: デバッグ用，実験時削除
    private long count = -999999999;

    public SynResultTuple(int machineId, double avgTemp, long timestamp, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        this.machineId = machineId;
        this.avgTemp = avgTemp;
        this.timestamp = timestamp;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public SynResultTuple(int machineId, double avgTemp, long timestamp) {
        this.machineId = machineId;
        this.avgTemp = avgTemp;
        this.timestamp = timestamp;
    }

    public SynResultTuple(int machineId, double avgTemp, long count, long timestamp) {
        this.machineId = machineId;
        this.avgTemp = avgTemp;
        this.count = count;
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
        return "SynResultTuple{" +
                "machineId=" + machineId +
                ", avgTemp=" + avgTemp +
                ", count =" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}
