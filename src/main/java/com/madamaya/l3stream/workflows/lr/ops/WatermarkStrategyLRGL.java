package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

import java.util.*;

public class WatermarkStrategyLRGL implements WatermarkStrategy<LinearRoadInputTupleGL> {
    private int partitionNum;
    private int parallelism;

    public WatermarkStrategyLRGL(int partitionNum, int parallelism) {
        this.partitionNum = partitionNum;
        this.parallelism = parallelism;
    }

    @Override
    public TimestampAssigner<LinearRoadInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTupleGL>() {
            @Override
            public long extractTimestamp(LinearRoadInputTupleGL linearRoadInputTuple, long l) {
                return Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        if (parallelism > 1) {
            return new WatermarkGenerator<LinearRoadInputTupleGL>() {
                @Override
                public void onEvent(LinearRoadInputTupleGL linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds() - 1));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

                }
            };
        } else {
            return new WatermarkGenerator<LinearRoadInputTupleGL>() {
                HashMap<Integer, Long> hm = new HashMap<>();

                @Override
                public void onEvent(LinearRoadInputTupleGL linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
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
    }

    private static long findMinimumWM(Map<Integer, Long> m) {
        List<Long> vList = new ArrayList<>(m.values());
        Collections.sort(vList);
        return vList.get(0);
    }
}
