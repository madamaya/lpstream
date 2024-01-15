package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinSynL3 implements JoinFunction<SynPowerTuple, SynTempTuple, SynJoinedTuple> {
    @Override
    public SynJoinedTuple join(SynPowerTuple synPowerTuple, SynTempTuple synTempTuple) throws Exception {
        SynJoinedTuple tuple = new SynJoinedTuple(
                synTempTuple.getMachineId(),
                synTempTuple.getSensorId(),
                synTempTuple.getTemperature(),
                synPowerTuple.getPower(),
                synTempTuple.getLog(),
                synPowerTuple.getLog(),
                Math.max(synTempTuple.getTimestamp(), synPowerTuple.getTimestamp())
        );
        return tuple;
    }
}
