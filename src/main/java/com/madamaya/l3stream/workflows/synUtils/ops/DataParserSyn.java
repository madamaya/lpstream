package com.madamaya.l3stream.workflows.synUtils.ops;

import com.madamaya.l3stream.workflows.synUtils.objects.___SynInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserSyn implements MapFunction<ObjectNode, ___SynInputTuple> {

    @Override
    public ___SynInputTuple map(ObjectNode jNode) throws Exception {
        if (jNode == null) {
            throw new UnknownError("SynInputTuple: map(), jNode == null");
        } else if (jNode.get("value").get("asin") == null) {
            throw new UnknownError("SynInputTuple: map(), jNode.get(\"asin\") == null\ntuple = " + jNode);
        }
        String asin = jNode.get("value").get("asin").textValue();
        Double overall = jNode.get("value").get("overall").doubleValue();
        return new ___SynInputTuple(asin, overall, System.currentTimeMillis());
    }
}
