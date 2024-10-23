package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignYSBL3 implements MapFunction<YSBInternalTuple, YSBInternalTuple> {
    @Override
    public YSBInternalTuple map(YSBInternalTuple in) throws Exception {
        YSBInternalTuple out = new YSBInternalTuple(in);
        return out;
    }
}
