package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Pattern;

public class DataParserLRL3 implements MapFunction<L3StreamInput<String>, LinearRoadInputTuple> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public LinearRoadInputTuple map(L3StreamInput<String> input) throws Exception {
        String line = input.getValue();
        String[] elements = delimiter.split(line.trim());
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
        tuple.setPartitionID(jNode.get("metadata").get("partition").asInt());

        return tuple;
    }
}
