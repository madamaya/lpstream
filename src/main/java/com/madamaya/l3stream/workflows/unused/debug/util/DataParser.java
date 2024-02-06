package com.madamaya.l3stream.workflows.unused.debug.util;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParser implements MapFunction<ObjectNode, Tuple3<Integer, Integer, Integer>> {

    @Override
    public Tuple3<Integer, Integer, Integer> map(ObjectNode jsonNodes) throws Exception {
        String[] elements = jsonNodes.get("value").textValue().split(",");

        if (elements.length != 3) {
            throw new IllegalArgumentException();
        }

        return Tuple3.of(Integer.parseInt(elements[0]), Integer.parseInt(elements[1]), Integer.parseInt(elements[2]));
    }
}
