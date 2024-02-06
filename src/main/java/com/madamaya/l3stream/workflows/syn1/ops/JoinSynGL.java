package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.*;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinSynGL implements JoinFunction<SynPowerTupleGL, SynTempTupleGL, SynJoinedTupleGL> {
    @Override
    public SynJoinedTupleGL join(SynPowerTupleGL synPowerTuple, SynTempTupleGL synTempTuple) throws Exception {
        SynJoinedTupleGL tuple = new SynJoinedTupleGL(
                synTempTuple.getMachineId(),
                synTempTuple.getSensorId(),
                synTempTuple.getTemperature(),
                synPowerTuple.getPower(),
                synTempTuple.getLog(),
                synPowerTuple.getLog(),
                Math.max(synTempTuple.getTimestamp(), synPowerTuple.getTimestamp()),
                System.nanoTime() - Math.max(synTempTuple.getDominantOpTime(), synPowerTuple.getDominantOpTime()),
                Math.max(synTempTuple.getKafkaAppendTime(), synPowerTuple.getKafkaAppendTime()),
                Math.max(synTempTuple.getStimulus(), synPowerTuple.getStimulus())
        );
        GenealogJoinHelper.INSTANCE.annotateResult(synPowerTuple, synTempTuple, tuple);
        return tuple;
    }
}
