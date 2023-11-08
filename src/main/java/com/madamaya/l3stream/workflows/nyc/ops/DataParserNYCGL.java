package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.glCommons.InputGL;
import com.madamaya.l3stream.glCommons.JsonNodeGL;
import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.SimpleDateFormat;

public class DataParserNYCGL implements MapFunction<StringGL, NYCInputTupleGL> {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public NYCInputTupleGL map(StringGL input) throws Exception {
        // JsonNode jNode = jNodeGL.getJsonNode();

        // String line = jNode.get("value").textValue();

        NYCInputTupleGL out = new NYCInputTupleGL(input.getString(), input.getStimulus(), sdf);
        GenealogMapHelper.INSTANCE.annotateResult(input, out);

        return out;
    }
}
