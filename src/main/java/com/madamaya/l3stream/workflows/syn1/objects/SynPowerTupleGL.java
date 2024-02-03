package com.madamaya.l3stream.workflows.syn1.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynPowerTupleGL extends SynPowerTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynPowerTupleGL(int type) {
        super(type);
    }

    public SynPowerTupleGL(int type, int machineId, double power, String log, long timestamp, long kafkaAppendTime, long stimulus) {
        super(type, machineId, power, log, timestamp, kafkaAppendTime, stimulus);
    }

    public SynPowerTupleGL(int type, int machineId, double power, String log, long timestamp) {
        super(type, machineId, power, log, timestamp);
    }

    public SynPowerTupleGL(SynPowerTupleGL tuple) {
        super(tuple.getType(), tuple.getMachineId(), tuple.getPower(), tuple.getLog(), tuple.getTimestamp(), tuple.getKafkaAppendTime(), tuple.getStimulus());
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
