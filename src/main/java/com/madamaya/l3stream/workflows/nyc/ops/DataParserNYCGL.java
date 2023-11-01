package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.glCommons.InputGL;
import com.madamaya.l3stream.glCommons.JsonNodeGL;
import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserNYCGL implements MapFunction<InputGL<String>, NYCInputTupleGL> {

    @Override
    public NYCInputTupleGL map(InputGL<String> input) throws Exception {
        // JsonNode jNode = jNodeGL.getJsonNode();

        // String line = jNode.get("value").textValue();

        NYCInputTupleGL out = new NYCInputTupleGL(input.getValue(), input.getStimulus());
        GenealogMapHelper.INSTANCE.annotateResult(input, out);

        return out;
    }
}
