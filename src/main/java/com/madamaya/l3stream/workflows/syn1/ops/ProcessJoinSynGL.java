package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class ProcessJoinSynGL extends ProcessJoinFunction<SynPowerTupleGL, SynTempTupleGL, SynJoinedTupleGL> {
    @Override
    public void processElement(SynPowerTupleGL synPowerTuple, SynTempTupleGL synTempTuple, ProcessJoinFunction<SynPowerTupleGL, SynTempTupleGL, SynJoinedTupleGL>.Context context, Collector<SynJoinedTupleGL> collector) throws Exception {
        SynJoinedTupleGL tuple = new SynJoinedTupleGL(
                synTempTuple.getMachineId(),
                synTempTuple.getSensorId(),
                synTempTuple.getTemperature(),
                synPowerTuple.getPower(),
                synTempTuple.getLog(),
                synPowerTuple.getLog(),
                Math.max(synTempTuple.getTimestamp(), synPowerTuple.getTimestamp()),
                Math.max(synTempTuple.getKafkaAppendTime(), synPowerTuple.getKafkaAppendTime()),
                Math.max(synTempTuple.getStimulus(), synPowerTuple.getStimulus())
        );
        GenealogJoinHelper.INSTANCE.annotateResult(synPowerTuple, synTempTuple, tuple);
        collector.collect(tuple);
    }
}
