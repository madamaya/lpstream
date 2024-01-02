package com.madamaya.l3stream.workflows.syn1.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynTempTupleGL extends SynTempTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynTempTupleGL(int type) {
        super(type);
    }

    public SynTempTupleGL(int type, int machineId, int sensorId, double temperature, String log, long timestamp, long stimulus) {
        super(type, machineId, sensorId, temperature, log, timestamp, stimulus);
    }

    public SynTempTupleGL(int type, int machineId, int sensorId, double temperature, String log, long timestamp) {
        super(type, machineId, sensorId, temperature, log, timestamp);
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
