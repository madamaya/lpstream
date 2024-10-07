package com.madamaya.l3stream.workflows.syn10.objects;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynTempTestTupleGL extends SynTempTestTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynTempTestTupleGL(SynTempTupleGL tuple, double maxTemp) {
        super(tuple, maxTemp);
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
