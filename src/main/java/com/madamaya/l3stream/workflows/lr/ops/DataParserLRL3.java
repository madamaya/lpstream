package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class DataParserLRL3 implements MapFunction<KafkaInputString, LinearRoadInputTuple> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public LinearRoadInputTuple map(KafkaInputString input) throws Exception {
        String inputStr = input.getStr();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();
        String[] elements = delimiter.split(line);
        LinearRoadInputTuple tuple = new LinearRoadInputTuple(
                Integer.valueOf(elements[0]),
                Long.valueOf(elements[1]),
                Integer.valueOf(elements[2]),
                Integer.valueOf(elements[3]),
                Integer.valueOf(elements[4]),
                Integer.valueOf(elements[5]),
                Integer.valueOf(elements[6]),
                Integer.valueOf(elements[7]),
                Integer.valueOf(elements[8])
        );
        tuple.setKey(String.valueOf(tuple.getVid()));
        return tuple;
    }
}
