package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class PowerParserSynL3 implements MapFunction<KafkaInputString, SynPowerTuple> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public SynPowerTuple map(KafkaInputString input) throws Exception {
        String inputStr = input.getStr();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        if (type == 0) {
            return new SynPowerTuple(type);
        } else {
            SynPowerTuple tuple = new SynPowerTuple(
                    type,
                    Integer.parseInt(elements[1]),
                    Double.parseDouble(elements[2]),
                    elements[3],
                    Long.parseLong(elements[4])
            );
            return tuple;
        }
    }
}
