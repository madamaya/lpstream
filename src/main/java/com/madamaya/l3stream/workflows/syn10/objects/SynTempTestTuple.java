package com.madamaya.l3stream.workflows.syn10.objects;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;

public class SynTempTestTuple extends SynTempTuple {

    private double maxTemp;

    public SynTempTestTuple(SynTempTuple tuple, double maxTemp) {
        super(tuple);
        this.maxTemp = maxTemp;
    }

    public double getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(double maxTemp) {
        this.maxTemp = maxTemp;
    }

    @Override
    public String toString() {
        return "SynTempTuple{" +
                "machineId=" + getMachineId() +
                ", sensorId=" + getSensorId() +
                ", temperature=" + getTemperature() +
                ", maxTemperature=" + getMaxTemp() +
                ", log='" + getLog() + '\'' +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
