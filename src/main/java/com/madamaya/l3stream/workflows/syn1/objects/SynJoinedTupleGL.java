package com.madamaya.l3stream.workflows.syn1.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynJoinedTupleGL extends SynJoinedTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynJoinedTupleGL(int machineId, int sensorId, double temperature, double power, String logTemp, String logPower, long timestamp, long stimulus) {
        super(machineId, sensorId, temperature, power, logTemp, logPower, timestamp, stimulus);
    }

    public SynJoinedTupleGL(int machineId, int sensorId, double temperature, double power, String logTemp, String logPower, long timestamp) {
        super(machineId, sensorId, temperature, power, logTemp, logPower, timestamp);
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        this.gdata.init(genealogTupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }
}
