package com.madamaya.l3stream.workflows.unused.syn6.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinSynGL6 implements JoinFunction<SynPowerTupleGL, SynTempTupleGL, SynJoinedTupleGL> {
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
                Math.max(synTempTuple.getKafkaAppendTime(), synPowerTuple.getKafkaAppendTime()),
                System.nanoTime() - Math.max(synTempTuple.getStimulus(), synPowerTuple.getStimulus())
        );
        GenealogJoinHelper.INSTANCE.annotateResult(synPowerTuple, synTempTuple, tuple);
        return tuple;
    }
}
