package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.glCommons.ObjectNodeGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserNYCGL implements MapFunction<ObjectNodeGL, NYCInputTupleGL> {

    @Override
    public NYCInputTupleGL map(ObjectNodeGL jNodeGL) throws Exception {
        ObjectNode jNode = jNodeGL.getObjectNode();

        String line = jNode.get("value").textValue();

        NYCInputTupleGL out = new NYCInputTupleGL(line, jNodeGL.getStimulus());
        GenealogMapHelper.INSTANCE.annotateResult(jNodeGL, out);

        return out;
    }
}
