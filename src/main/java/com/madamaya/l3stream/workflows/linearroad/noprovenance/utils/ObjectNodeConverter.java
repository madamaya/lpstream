package com.madamaya.l3stream.workflows.linearroad.noprovenance.utils;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class ObjectNodeConverter implements MapFunction<ObjectNode, LinearRoadInputTuple> {

    @Override
    public LinearRoadInputTuple map(ObjectNode jNode) throws Exception {
        String line = jNode.get("value").textValue();
        LinearRoadInputTuple tuple = LinearRoadInputTuple.fromReading(line);
        long newId = tuple.getVid();
        int taskIndex = 0;
        tuple.setVid(newId);
        tuple.setKey(String.valueOf(newId));
        tuple.setXway(taskIndex);
        return tuple;
    }
}
