package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Pattern;

public class DataParserLR implements MapFunction<ObjectNode, LinearRoadInputTuple> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public LinearRoadInputTuple map(ObjectNode jNode) throws Exception {
        long stimulus = System.nanoTime();

        String line = jNode.get("value").textValue();
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
                Integer.valueOf(elements[8]),
                stimulus
        );
        tuple.setKey(String.valueOf(tuple.getVid()));
        return tuple;
    }
}
