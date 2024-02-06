package com.madamaya.l3stream.workflows.unused.syn6.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinSyn6 implements JoinFunction<SynPowerTuple, SynTempTuple, SynJoinedTuple> {
    @Override
    public SynJoinedTuple join(SynPowerTuple synPowerTuple, SynTempTuple synTempTuple) throws Exception {
        SynJoinedTuple tuple = new SynJoinedTuple(
                synTempTuple.getMachineId(),
                synTempTuple.getSensorId(),
                synTempTuple.getTemperature(),
                synPowerTuple.getPower(),
                synTempTuple.getLog(),
                synPowerTuple.getLog(),
                Math.max(synTempTuple.getTimestamp(), synPowerTuple.getTimestamp()),
                Math.max(synTempTuple.getDominantOpTime(), synPowerTuple.getDominantOpTime()),
                Math.max(synTempTuple.getKafkaAppendTime(), synPowerTuple.getKafkaAppendTime()),
                System.nanoTime() - Math.max(synTempTuple.getStimulus(), synPowerTuple.getStimulus())
        );
        return tuple;
    }
}
