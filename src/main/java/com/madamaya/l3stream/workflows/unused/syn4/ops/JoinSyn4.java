package com.madamaya.l3stream.workflows.unused.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinSyn4 extends ProcessJoinFunction<SynPowerTuple, SynTempTuple, SynJoinedTuple> {
    @Override
    public void processElement(SynPowerTuple synPowerTuple, SynTempTuple synTempTuple, ProcessJoinFunction<SynPowerTuple, SynTempTuple, SynJoinedTuple>.Context context, Collector<SynJoinedTuple> collector) throws Exception {
        SynJoinedTuple tuple = new SynJoinedTuple(
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
        collector.collect(tuple);
    }
}