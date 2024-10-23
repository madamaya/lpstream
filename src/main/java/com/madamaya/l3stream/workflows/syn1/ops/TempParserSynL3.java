package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class TempParserSynL3 implements MapFunction<KafkaInputString, SynTempTuple> {
    private static final Pattern delimiter = Pattern.compile(",");

     @Override
    public SynTempTuple map(KafkaInputString input) throws Exception {
        String inputStr = input.getStr();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        if (type == 0) {
            SynTempTuple tuple = new SynTempTuple(
                    type,
                    Integer.parseInt(elements[1]),
                    Integer.parseInt(elements[2]),
                    Double.parseDouble(elements[3]),
                    elements[4],
                    Long.parseLong(elements[5])
            );
            return tuple;
        } else {
            return new SynTempTuple(type);
        }
    }
}
