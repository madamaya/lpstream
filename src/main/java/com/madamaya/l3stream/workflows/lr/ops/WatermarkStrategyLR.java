package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

import java.util.*;

public class WatermarkStrategyLR implements WatermarkStrategy<LinearRoadInputTuple> {
    private int partitionNum;

    public WatermarkStrategyLR(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public TimestampAssigner<LinearRoadInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTuple>() {
            @Override
            public long extractTimestamp(LinearRoadInputTuple linearRoadInputTuple, long l) {
                return Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<LinearRoadInputTuple>() {
            HashMap<Integer, Long> hm = new HashMap<>();

            @Override
            public void onEvent(LinearRoadInputTuple linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                hm.put(linearRoadInputTuple.getPartitionID(), Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds() - 1);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                if (hm.size() == partitionNum) {
                    watermarkOutput.emitWatermark(new Watermark(findMinimumWM(hm)));
                }
            }
        };
    }

    private static long findMinimumWM(Map<Integer, Long> m) {
        List<Long> vList = new ArrayList<>(m.values());
        Collections.sort(vList);
        return vList.get(0);
    }
}
