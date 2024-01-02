package com.madamaya.l3stream.workflows.syn1.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynPowerTupleGL extends SynPowerTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynPowerTupleGL(int type) {
        super(type);
    }

    public SynPowerTupleGL(int type, int machineId, double power, String log, long timestamp, long stimulus) {
        super(type, machineId, power, log, timestamp, stimulus);
    }

    public SynPowerTupleGL(int type, int machineId, double power, String log, long timestamp) {
        super(type, machineId, power, log, timestamp);
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
